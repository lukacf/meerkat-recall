//! A retirement receipt may preserve graph visibility across an explicit S1
//! regrouping. Old attribution/support rows stay intact; arbitrary new support
//! is never sufficient to suppress an old dependency.
use super::{
    ReadAuthorization, ReadEligibility, Repository, SourceEffectSupport,
    SourcePublicationRepository, SourceRepository, SourceVisibility, StorageResult,
};
use crate::historical_adoption::HistoricalRetirementReceipt;
use crate::source_publication::{operation_key, stable_key};
impl ReadEligibility {
    #[inline(never)]
    pub(super) fn historical_regrouped_support<'a>(
        &'a self,
        old: &'a SourceEffectSupport,
        candidates: &'a [SourceEffectSupport],
        authorize: Option<&'a ReadAuthorization<'a>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<bool>> + Send + 'a>> {
        Box::pin(async move {
            // This helper decides whether an obsolete entry can be suppressed.
            // An already-current original is the authority that must be retained.
            if self
                .head(&old.space_id, &old.source_id)
                .await?
                .is_some_and(|head| {
                    head.lifecycle == SourceVisibility::Active
                        && head.published_revision.as_deref() == Some(old.source_revision.as_str())
                })
            {
                return Ok(false);
            }
            // Every intermediate support remains immutable. Walk only exact
            // retirement-declared edges; bounded traversal cannot loop on forged
            // cycles or amplify arbitrary caller-provided replacement populations.
            if candidates.len() > 4096 {
                return Ok(false);
            }
            let mut pending = std::collections::VecDeque::from([old]);
            let mut visited = std::collections::BTreeSet::new();
            let mut edges = 0usize;
            while let Some(retired_support) = pending.pop_front() {
                let Some(original) = &retired_support.historical_unknown else {
                    return Ok(false);
                };
                let key = (
                    retired_support.source_id.clone(),
                    retired_support.source_revision.clone(),
                    original.commit_id.clone(),
                );
                if !visited.insert(key) {
                    continue;
                }
                if visited.len() > 256
                    || !self
                        .historical_support_receipt_is_retained(retired_support)
                        .await?
                {
                    return Ok(false);
                }
                let Some(head) = self
                    .retired_source_head(old, retired_support, authorize)
                    .await?
                else {
                    return Ok(false);
                };
                if head.lifecycle == elephant_types::publication::SourceVisibility::Active {
                    if self
                        .current_historical_replacement(old, retired_support, candidates, &head)
                        .await?
                    {
                        return Ok(true);
                    }
                    continue;
                }
                if head.lifecycle != elephant_types::publication::SourceVisibility::Retracted
                    || head.published_revision.is_some()
                {
                    continue;
                }
                let Some(retirement) = self
                    .retained_retirement(old, retired_support, &head)
                    .await?
                else {
                    return Ok(false);
                };
                if retirement.original_commit_id != original.commit_id {
                    // A later actual publication of the same source may precede
                    // retirement. Preserve and verify both immutable certificates,
                    // then traverse from the exact publication the retirement owns.
                    let Some(latest) = candidates.iter().find(|candidate| {
                        candidate.source_id == retired_support.source_id
                            && candidate.record_id == old.record_id
                            && candidate.space_id == old.space_id
                            && candidate
                                .historical_unknown
                                .as_ref()
                                .is_some_and(|history| {
                                    history.commit_id == retirement.original_commit_id
                                })
                    }) else {
                        return Ok(false);
                    };
                    if !self.historical_support_receipt_is_retained(latest).await? {
                        return Ok(false);
                    }
                    pending.push_back(latest);
                    continue;
                }
                if let Some(result) = self
                    .advance_retirement_replacements(
                        &retirement,
                        old,
                        candidates,
                        authorize,
                        &mut pending,
                        &mut edges,
                    )
                    .await?
                {
                    return Ok(result);
                }
            }
            Ok(false)
        })
    }
    #[inline(never)]
    fn current_historical_replacement<'a>(
        &'a self,
        old: &'a SourceEffectSupport,
        retired_support: &'a SourceEffectSupport,
        candidates: &'a [SourceEffectSupport],
        head: &'a elephant_types::publication::SourceHead,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<bool>> + Send + 'a>> {
        Box::pin(async move {
            // Same-source historical refresh is valid only with the exact
            // currently published immutable certificate for this record.
            if let Some(current) = candidates.iter().find(|candidate| {
                candidate.source_id == retired_support.source_id
                    && candidate.record_id == old.record_id
                    && candidate.space_id == old.space_id
                    && head.published_revision.as_deref()
                        == Some(candidate.source_revision.as_str())
            }) {
                if self.historical_support_receipt_is_retained(current).await?
                    && self.historical_support_is_current(current).await?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    #[inline(never)]
    fn retired_source_head<'a>(
        &'a self,
        old: &'a SourceEffectSupport,
        retired_support: &'a SourceEffectSupport,
        authorize: Option<&'a ReadAuthorization<'a>>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = StorageResult<Option<elephant_types::publication::SourceHead>>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(source) = SourceRepository::new(self.storage.clone())
                .get(&old.space_id, &retired_support.source_id)
                .await?
            else {
                return Ok(None);
            };
            if source.base.deleted_at.is_some()
                || authorize.is_some_and(|allow| !allow(&source.base))
            {
                return Ok(None);
            }
            // Track even retired source envelopes for end-of-read revalidation.
            let _ = self.source(&source, authorize.unwrap_or(&|_| true)).await?;
            self.head(&old.space_id, &retired_support.source_id).await
        })
    }

    #[inline(never)]
    fn retained_retirement<'a>(
        &'a self,
        old: &'a SourceEffectSupport,
        retired_support: &'a SourceEffectSupport,
        head: &'a elephant_types::publication::SourceHead,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = StorageResult<Option<HistoricalRetirementReceipt>>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let repository = SourcePublicationRepository::new(self.storage.clone());
            // The current head identifies the exact retirement operation. Its
            // immutable primary receipt is selected by the actual operation's
            // request fingerprint, then every declaration is cross-bound.
            let Some(operation) = repository
                .read_control::<crate::source_publication::StoredOperation>(
                    "source_operation",
                    &head.operation_id,
                    &old.space_id,
                )
                .await?
            else {
                return Ok(None);
            };
            let commit_id = &operation.request_fingerprint;
            if operation.input_fingerprint != *commit_id
                || operation.receipt.source_id != retired_support.source_id
                || operation.receipt.state != elephant_types::publication::OperationState::Retracted
                || operation.receipt.source_generation != head.generation
                || operation.receipt.operation_id != head.operation_id
                || operation_key(&old.space_id, commit_id) != head.operation_id
            {
                return Ok(None);
            }
            let Some(commit) = repository
                .read_control::<crate::canonical_commit::CanonicalCommitReceipt>(
                    "source_commit_receipt",
                    &stable_key(&[&old.space_id, commit_id]),
                    &old.space_id,
                )
                .await?
            else {
                return Ok(None);
            };
            drop(repository);
            let Some(output) = commit.replay_output.clone() else {
                return Ok(None);
            };
            let retirement: HistoricalRetirementReceipt =
                serde_json::from_value(output).map_err(|_| {
                    crate::StorageError::DataIntegrityError(
                        "historical retirement proof invalid".into(),
                    )
                })?;
            if commit.commit_id != *commit_id
                || commit.space_id != old.space_id
                || commit.source_id.as_ref() != Some(&retired_support.source_id)
                || commit.source_ids != [retired_support.source_id.clone()]
                || commit.publication_authority.is_some()
                || commit.historical_import.is_some()
                || !commit.maintenance_authority.is_empty()
                || !commit.effect_ids.is_empty()
                || !commit.extension_ids.is_empty()
                || !commit.outbox_ids.is_empty()
                || commit.plan_fingerprint != retirement.plan_sha256
                || commit.source_revision.is_some()
                || retirement.commit_id != *commit_id
                || retirement.source_id != retired_support.source_id
                || retirement.operation != operation.receipt
                || stable_key(&[
                    "historical-s1-retirement-v1",
                    &retirement.plan_sha256,
                    &retired_support.source_id.to_string(),
                ]) != *commit_id
            {
                return Ok(None);
            }
            Ok(Some(retirement))
        })
    }

    #[inline(never)]
    fn advance_retirement_replacements<'a, 'candidate: 'a>(
        &'a self,
        retirement: &'a HistoricalRetirementReceipt,
        old: &'a SourceEffectSupport,
        candidates: &'candidate [SourceEffectSupport],
        authorize: Option<&'a ReadAuthorization<'a>>,
        pending: &'a mut std::collections::VecDeque<&'candidate SourceEffectSupport>,
        edges: &'a mut usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<Option<bool>>> + Send + 'a>>
    {
        Box::pin(async move {
            let Some(replacements) = retirement.graph_replacements.get(&old.record_id) else {
                return Ok(None);
            };
            *edges = edges.saturating_add(replacements.len());
            if *edges > 4096 {
                return Ok(Some(false));
            }
            for replacement in replacements {
                let Some(candidate) = candidates.iter().find(|entry| {
                    entry.source_id == replacement.source_id
                        && entry.source_revision == replacement.source_revision
                        && entry.record_id == old.record_id
                        && entry.space_id == old.space_id
                        && entry.historical_unknown.as_ref().is_some_and(|history| {
                            let replacement_adoption_matches =
                                history.plan_id == retirement.adoption_plan_id;
                            history.commit_id == replacement.commit_id
                                && replacement_adoption_matches
                                && history.snapshot_sha256 == retirement.original_snapshot_sha256
                        })
                }) else {
                    return Ok(Some(false));
                };
                if !self
                    .historical_support_receipt_is_retained(candidate)
                    .await?
                {
                    return Ok(Some(false));
                }
                if self.historical_support_is_current(candidate).await? {
                    let Some(current) = SourceRepository::new(self.storage.clone())
                        .get(&old.space_id, &candidate.source_id)
                        .await?
                    else {
                        return Ok(Some(false));
                    };
                    return self
                        .source(&current, authorize.unwrap_or(&|_| true))
                        .await
                        .map(Some);
                }
                pending.push_back(candidate);
            }
            Ok(None)
        })
    }
}
