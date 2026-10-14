//! Reuse only caller-supplied query vectors, never source content or results.
//! Current provider authority and a freshly captured canonical epoch fence each use.
//! A vector describes only the caller input; unrelated graph writes do not change it.
use super::{binding_error, embedding_resolution_error, unavailable};
use crate::handlers::ToolContext;
use elephant_embeddings::{
    governed::policy_for_binding, local_model_cache::VerifiedLocalModelCache, EmbeddingPolicy,
};
use elephant_pipeline::provider_dispatch::ProviderDispatchContext;
use elephant_storage::{canonical_commit::CanonicalRepository, SpaceConfigRepository};
use elephant_types::{
    provider_binding::{ModelBindingProvenance, ModelOperation, ResolvedModelBinding},
    public_error::{ErrorCode, PublicError},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::{Duration, Instant},
};
use tokio::sync::OnceCell;

const MAX_ENTRIES: usize = 128;
const MAX_RESERVED_BYTES: usize = 16 * 1024 * 1024;
const ENTRY_OVERHEAD: usize = 1024;
const TTL: Duration = Duration::from_secs(300);
type Key = [u8; 32];

struct Value {
    vector: Box<[f32]>,
    filled_at: Instant,
}
struct Entry {
    // StorageClient clones share this existing resource owner; a new store
    // lifetime has a different allocation. Weak ownership never retains a store.
    owner: Weak<VerifiedLocalModelCache>,
    created: Instant,
    reserved_bytes: usize,
    value: OnceCell<Value>,
}
impl Entry {
    fn fresh(&self, now: Instant) -> bool {
        self.owner.strong_count() != 0
            && now.saturating_duration_since(
                self.value
                    .get()
                    .map_or(self.created, |value| value.filled_at),
            ) < TTL
    }
}
#[derive(Default)]
struct Cache {
    entries: HashMap<Key, Arc<Entry>>,
}
impl Cache {
    fn entry(
        &mut self,
        key: Key,
        owner: &Arc<VerifiedLocalModelCache>,
        dims: usize,
        now: Instant,
    ) -> Option<Arc<Entry>> {
        // Never evict a filling/waited-on entry: detached in-flight cells would
        // evade the entry/byte bounds. Saturation takes the normal uncached path.
        self.entries
            .retain(|_, entry| Arc::strong_count(entry) > 1 || entry.fresh(now));
        if let Some(entry) = self.entries.get(&key) {
            return entry.fresh(now).then(|| entry.clone());
        }
        let reserved_bytes = dims
            .checked_mul(size_of::<f32>())?
            .checked_add(ENTRY_OVERHEAD)?;
        if reserved_bytes > MAX_RESERVED_BYTES {
            return None;
        }
        while self.entries.len() >= MAX_ENTRIES
            || self
                .entries
                .values()
                .map(|entry| entry.reserved_bytes)
                .sum::<usize>()
                > MAX_RESERVED_BYTES - reserved_bytes
        {
            let oldest = self
                .entries
                .iter()
                .filter(|(_, entry)| Arc::strong_count(entry) == 1)
                .min_by_key(|(_, entry)| entry.created)
                .map(|(key, _)| *key)?;
            self.entries.remove(&oldest);
        }
        let entry = Arc::new(Entry {
            owner: Arc::downgrade(owner),
            created: now,
            reserved_bytes,
            value: OnceCell::new(),
        });
        self.entries.insert(key, entry.clone());
        Some(entry)
    }
}

fn sorted<'a>(values: impl Iterator<Item = &'a String>) -> Vec<&'a str> {
    let mut values: Vec<_> = values.map(String::as_str).collect();
    values.sort_unstable();
    values
}
fn clearance_key(clearances: &[elephant_policy::Clearance]) -> serde_json::Value {
    // Preserve clearance order (including duplicate policy IDs); normalize only
    // explicitly unordered sets. No first-wins policy merge is introduced here.
    json!(clearances
        .iter()
        .map(|clearance| json!({
            "policy": clearance.policy_id,
            "level": clearance.max_level,
            "labels": sorted(clearance.labels.iter()),
            "handling": sorted(clearance.handling_allow.iter()),
        }))
        .collect::<Vec<_>>())
}
fn key(
    ctx: &ToolContext,
    space: &str,
    query: &str,
    policy: &EmbeddingPolicy,
    provenance: &ModelBindingProvenance,
    owner: &Arc<VerifiedLocalModelCache>,
) -> Result<Key, PublicError> {
    let principal = &ctx.principal;
    let subjects = principal.subject_allowlist.as_ref().map(|subjects| {
        let mut values: Vec<_> = subjects.iter().map(ToString::to_string).collect();
        values.sort_unstable();
        values
    });
    let verified = ctx
        .verified_caller()
        .map(|caller| {
            let mut claims = serde_json::to_value(caller.claims())?;
            claims["clearances"] = clearance_key(&caller.claims().clearances);
            Ok::<_, serde_json::Error>(json!({
                "claims": claims, "identity": caller.mapped_identity(),
            }))
        })
        .transpose()
        .map_err(|_| query_unavailable())?;
    let input = json!({
        "version": 2,
        "owner": Arc::as_ptr(owner) as usize,
        "space": space,
        "query_sha256": hex::encode(Sha256::digest(query.as_bytes())),
        "provider": policy.provider, "partition": policy.model, "dims": policy.dims,
        "provenance": provenance,
        "principal": {
            "space": principal.space_id,
            "scopes": sorted(principal.scopes.iter()),
            "clearances": clearance_key(&principal.clearances),
            "subjects": subjects,
            "purpose": sorted(principal.purpose.iter()),
        },
        "verified": verified,
    });
    // Retain only the digest. Neither claims, query text, credentials, source IDs
    // nor search results enter the cache. The weak allocation prevents ABA reuse.
    let encoded = serde_json::to_vec(&input).map_err(|_| query_unavailable())?;
    Ok(Sha256::digest(encoded).into())
}
fn query_unavailable() -> PublicError {
    PublicError::new(ErrorCode::Unavailable, "Query embedding is unavailable")
}
fn changed() -> PublicError {
    PublicError::new(
        ErrorCode::NotReady,
        "Query embedding authority changed during search; retry",
    )
}
// Private control flow: public NotReady has several causes and is never a
// retry classifier. Only an unchanged authority with a changed epoch can reprepare.
#[derive(Debug)]
pub(super) enum QueryError {
    Terminal(PublicError),
    EpochChanged,
}
impl From<PublicError> for QueryError {
    fn from(error: PublicError) -> Self {
        Self::Terminal(error)
    }
}
impl QueryError {
    pub(super) fn into_public(self) -> PublicError {
        match self {
            Self::Terminal(error) => error,
            Self::EpochChanged => changed(),
        }
    }
}
#[derive(Debug, Clone)]
pub(super) struct Authority {
    policy: EmbeddingPolicy,
    binding: ResolvedModelBinding,
}

pub(super) fn ensure_ready(ctx: &ToolContext) -> Result<(), PublicError> {
    if ctx.storage.recovery_status().ready {
        Ok(())
    } else {
        Err(PublicError::new(
            ErrorCode::NotReady,
            "Storage recovery or maintenance is in progress",
        ))
    }
}

async fn authority(ctx: &ToolContext, space: &str) -> Result<Authority, PublicError> {
    ensure_ready(ctx)?;
    if space != ctx.principal.space_id {
        return Err(PublicError::new(
            ErrorCode::ForbiddenSpace,
            "Query space is not permitted",
        ));
    }
    let binding = SpaceConfigRepository::new(ctx.storage.clone())
        .authorize_model_operation(space, ModelOperation::QueryEmbedding, None)
        .await
        .map_err(binding_error)?;
    let policy = policy_for_binding(&binding).map_err(|_| {
        PublicError::new(
            ErrorCode::ProviderUnapproved,
            "Query embedding profile is unavailable",
        )
    })?;
    ensure_ready(ctx)?;
    Ok(Authority { policy, binding })
}

pub(super) async fn verify_authority(
    ctx: &ToolContext,
    space: &str,
    original: &Authority,
) -> Result<(), PublicError> {
    let current = authority(ctx, space).await?;
    // Include connection/account/credential generation, not just partition or
    // profile. A reprepare must never select a replacement approval or transport.
    if current.binding != original.binding || current.policy != original.policy {
        return Err(changed());
    }
    Ok(())
}

#[expect(
    clippy::ref_option,
    reason = "Compare the borrowed original epoch representation with the current observation"
)]
async fn current_typed(
    ctx: &ToolContext,
    space: &str,
    original: &Authority,
    epoch: &Option<String>,
) -> Result<(), QueryError> {
    verify_authority(ctx, space, original).await?;
    let observed = CanonicalRepository::new(ctx.storage.clone())
        .observe_epoch(space)
        .await
        .map_err(unavailable)?;
    ensure_ready(ctx)?;
    if observed != *epoch {
        return Err(QueryError::EpochChanged);
    }
    Ok(())
}

// Retain the low-level rejection contract and its existing regression controls.
#[cfg(test)]
#[expect(
    clippy::ref_option,
    reason = "Preserve the low-level original epoch rejection control"
)]
async fn current(
    ctx: &ToolContext,
    space: &str,
    policy: &EmbeddingPolicy,
    provenance: &ModelBindingProvenance,
    epoch: &Option<String>,
) -> Result<(), PublicError> {
    let original = authority(ctx, space).await?;
    if original.policy != *policy || original.binding.provenance != *provenance {
        return Err(changed());
    }
    current_typed(ctx, space, &original, epoch)
        .await
        .map_err(QueryError::into_public)
}

#[cfg(test)]
async fn embed(
    ctx: &ToolContext,
    space: &str,
    query: &str,
) -> Result<(EmbeddingPolicy, Vec<f32>), PublicError> {
    Box::pin(embed_for_search(ctx, space, query, &mut None))
        .await
        .map_err(QueryError::into_public)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub(super) async fn embed_for_search(
    ctx: &ToolContext,
    space: &str,
    query: &str,
    selected: &mut Option<Authority>,
) -> Result<(EmbeddingPolicy, Vec<f32>), QueryError> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    let current = authority(ctx, space).await?;
    if let Some(original) = selected.as_ref() {
        if original.binding != current.binding || original.policy != current.policy {
            return Err(changed().into());
        }
    } else {
        *selected = Some(current.clone());
    }
    // Fresh authorization precedes lookup, including on a ready cache hit.
    // A fresh provider handle retains per-physical-attempt credential/approval
    // checks; no physical call or usage receipt is fabricated on a cache hit.
    let (policy, provider, provenance) =
        ProviderDispatchContext::new(SpaceConfigRepository::new(ctx.storage.clone()), space)
            .embedding(ModelOperation::QueryEmbedding)
            .await
            .map_err(embedding_resolution_error)?;
    if policy != current.policy || provenance != current.binding.provenance {
        return Err(changed().into());
    }
    let epoch = CanonicalRepository::new(ctx.storage.clone())
        .observe_epoch(space)
        .await
        .map_err(unavailable)?;
    #[cfg(test)]
    tests::epoch_observed();
    // The epoch belongs to this search, not the immutable vector memo identity.
    // Restore/erasure still invalidate in-progress searches and current source
    // reads; a reused caller vector carries neither results nor read authority.
    let owner = ctx.storage.local_models();
    let key = key(ctx, space, query, &policy, &provenance, &owner)?;
    let entry = CACHE
        .get_or_init(Mutex::default)
        .lock()
        .ok()
        .and_then(|mut cache| cache.entry(key, &owner, policy.dims, Instant::now()));
    let fill = || async {
        current_typed(ctx, space, &current, &epoch).await?;
        let mut vectors = provider.embed(&[query.to_owned()]).await.map_err(|error| {
            if error.to_string().contains("provider_authorization") {
                PublicError::new(
                    ErrorCode::ProviderUnapproved,
                    "Query embedding is unavailable",
                )
            } else {
                query_unavailable()
            }
        })?;
        if vectors.len() != 1
            || vectors[0].len() != policy.dims
            || vectors[0].iter().any(|value| !value.is_finite())
        {
            return Err(QueryError::Terminal(query_unavailable()));
        }
        // Authorization/epoch failure must never install a successful fill.
        current_typed(ctx, space, &current, &epoch).await?;
        Ok(Value {
            vector: vectors.remove(0).into_boxed_slice(),
            filled_at: Instant::now(),
        })
    };
    let vector = if let Some(entry) = entry {
        // OnceCell cancellation/error leaves the cell empty. Each waiting caller
        // supplies its own freshly governed fill and rechecks authority below.
        let value = entry.value.get_or_try_init(fill).await?;
        if value.filled_at.elapsed() >= TTL {
            return Err(QueryError::Terminal(changed()));
        }
        value.vector.to_vec()
    } else {
        fill().await?.vector.into_vec()
    };
    // Applies equally to hits, the filling caller and callers that waited.
    current_typed(ctx, space, &current, &epoch).await?;
    Ok((policy, vector))
}

#[cfg(test)]
#[path = "scoped_query_vectors_tests.rs"]
mod tests;
