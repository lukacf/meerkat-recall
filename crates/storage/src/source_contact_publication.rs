//! Private contact authority persisted with source acceptance and consumed by the
//! canonical transaction. The transport cannot supply an arbitrary effect plan.

use std::collections::BTreeMap;

use elephant_types::publication::{
    OperationReceipt, OperationState, PublicationFence, SourceHead, SourceVisibility,
};
use elephant_types::records::Rel;
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use surrealdb::sql::Value;

use crate::canonical_commit::{
    CanonicalEffect, CanonicalPlan, CanonicalRecord, CanonicalRepository, SourceEffectSupport,
};
use crate::source_contact_intake::PreparedSourceContact;
use crate::source_publication::{
    acceptance_identity_key, conflict, control_thing, control_value, input_fingerprint, integrity,
    read_control_payload_tx, read_control_tx, stable_key, write_control_tx, AcceptSourceRevision,
    AcceptedContactCapability, SourceAcceptanceRetry, SourcePublicationRepository, StoredOperation,
    OPERATION_TABLE,
};
use crate::{EmbeddedMutation, StorageClient, StorageResult};

pub const CONTACT_TABLE: &str = "source_contact_intent";

/// Original authorized contribution, scoped to an acceptance, not just content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptedSourceContact {
    space_id: String,
    publication: PublicationFence,
    contact: PreparedSourceContact,
}

impl AcceptedSourceContact {
    pub(crate) fn new(head: &SourceHead, contact: PreparedSourceContact) -> StorageResult<Self> {
        let publication = PublicationFence {
            source_id: head.source_id.clone(),
            generation: head.generation.clone(),
            revision_id: head
                .desired_revision
                .clone()
                .ok_or_else(|| integrity("contact revision missing"))?,
            operation_id: head.operation_id.clone(),
        };
        contact.validate_seal(&head.space_id, &head.source_id, contact.digest())?;
        Ok(Self {
            space_id: head.space_id.clone(),
            publication,
            contact,
        })
    }

    fn key(&self) -> String {
        contact_key(&self.space_id, &self.publication)
    }

    pub(crate) fn write_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        if read_control_tx::<Self>(tx, CONTACT_TABLE, &self.key(), &self.space_id)?.is_some() {
            return Err(conflict("contact_intent_exists"));
        }
        write_control_tx(
            tx,
            CONTACT_TABLE,
            &self.key(),
            &self.space_id,
            &self.publication.source_id,
            self,
        )
    }

    pub(crate) fn append_write_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        bindings.extend(crate::bindings! {
            "accepted_contact_id" => control_thing(CONTACT_TABLE, &self.key()),
            "accepted_contact_record" => control_value(&self.space_id, &self.publication.source_id, self)?,
        });
        sql.push_str("CREATE $accepted_contact_id CONTENT $accepted_contact_record;\n");
        Ok(())
    }
}

pub fn contact_key(space: &str, publication: &PublicationFence) -> String {
    stable_key(&[
        space,
        &publication.source_id.to_string(),
        &publication.generation,
        &publication.operation_id,
        &publication.revision_id,
    ])
}

/// Exact private rows captured during preparation. Even absent contact authority
/// is checked again inside publication; presence cannot appear behind a plan.
#[derive(Clone)]
pub struct AcceptedContactState {
    space: String,
    publication: PublicationFence,
    operation: StoredOperation,
    intent: Option<AcceptedSourceContact>,
    operation_payload: String,
    intent_payload: Option<String>,
}

impl AcceptedContactState {
    pub(crate) async fn load(
        client: &StorageClient,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<Self> {
        let state = Self::observe(client, space, publication).await?;
        if state.operation.contact_digest.is_some() && state.intent.is_none() {
            return Err(conflict("accepted_contact_plan_unavailable"));
        }
        state.validate()?;
        Ok(state)
    }

    async fn observe(
        client: &StorageClient,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<Self> {
        let repo = SourcePublicationRepository::new(client.clone());
        let operation_payload = repo
            .read_control_payload(OPERATION_TABLE, &publication.operation_id, space)
            .await?
            .ok_or_else(|| integrity("accepted contact operation missing"))?;
        let operation = crate::source_publication::decode_payload(&operation_payload)?;
        let intent_payload = repo
            .read_control_payload(CONTACT_TABLE, &contact_key(space, publication), space)
            .await?;
        drop(repo);
        let intent = intent_payload
            .as_deref()
            .map(crate::source_publication::decode_payload)
            .transpose()?;
        let state = Self {
            space: space.to_owned(),
            publication: publication.clone(),
            operation,
            intent,
            operation_payload,
            intent_payload,
        };
        validate_accepted_operation(space, publication, &state.operation)?;
        // Missing historical plan is an explicit classification, never plain.
        if state.operation.contact_digest.is_none() || state.intent.is_some() {
            state.validate()?;
        }
        Ok(state)
    }

    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "AcceptedSourceContact.space_id is the persisted scope; AcceptedContactState.space is its captured authority for the same publication."
    )]
    fn validate(&self) -> StorageResult<()> {
        validate_accepted_operation(&self.space, &self.publication, &self.operation)?;
        match (&self.operation.contact_digest, &self.intent) {
            (None, None) => Ok(()),
            (Some(digest), Some(intent))
                if intent.space_id == self.space && intent.publication == self.publication =>
            {
                intent
                    .contact
                    .validate_seal(&self.space, &self.publication.source_id, digest)
            }
            _ => Err(integrity("accepted contact authority is incomplete")),
        }
    }

    pub(crate) fn independent_contribution_rows(
        &self,
        original_source: &elephant_types::records::Source,
    ) -> StorageResult<Vec<CanonicalRecord>> {
        self.validate()?;
        if self.operation.receipt.state != OperationState::Completed {
            return Err(conflict("contact_survival_original_not_completed"));
        }
        self.intent
            .as_ref()
            .ok_or_else(|| conflict("accepted_contact_plan_unavailable"))?
            .contact
            .independent_contribution_rows(original_source)
    }

    pub(crate) fn validate_plan(&self, plan: &CanonicalPlan) -> StorageResult<()> {
        if let Some(intent) = &self.intent {
            intent.contact.validate_plan(plan)?;
        }
        Ok(())
    }

    pub(crate) fn decision_effects(
        &self,
        plan: &CanonicalPlan,
    ) -> StorageResult<Vec<CanonicalEffect>> {
        self.intent.as_ref().map_or_else(
            || Ok(Vec::new()),
            |intent| intent.contact.decision_effects(plan),
        )
    }

    pub(crate) fn decision_outbox_ids(&self) -> Vec<RecordId> {
        self.intent
            .as_ref()
            .map_or_else(Vec::new, |intent| intent.contact.decision_outbox_ids())
    }

    pub(crate) fn certifies_effect(
        &self,
        effect: &CanonicalEffect,
        retirement: Option<&CanonicalEffect>,
    ) -> StorageResult<bool> {
        self.intent.as_ref().map_or(Ok(false), |intent| {
            intent.contact.certifies_effect(effect, retirement)
        })
    }

    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        if read_control_payload_tx(
            tx,
            OPERATION_TABLE,
            &self.publication.operation_id,
            &self.space,
        )?
        .as_ref()
            != Some(&self.operation_payload)
            || read_control_payload_tx(
                tx,
                CONTACT_TABLE,
                &contact_key(&self.space, &self.publication),
                &self.space,
            )? != self.intent_payload
        {
            return Err(conflict("accepted_contact_changed"));
        }
        if let Some(intent) = &self.intent {
            intent.contact.check_tx(tx)?;
        }
        Ok(())
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        bindings.extend(crate::bindings! {
            "contact_operation_id" => control_thing(OPERATION_TABLE, &self.publication.operation_id),
            "contact_operation_payload" => self.operation_payload.clone(),
            "contact_intent_id" => control_thing(CONTACT_TABLE, &contact_key(&self.space, &self.publication)),
            "contact_intent_payload" => self.intent_payload.clone().map_or(Value::None, Value::from),
        });
        sql.push_str("IF (SELECT VALUE payload FROM ONLY $contact_operation_id) != $contact_operation_payload OR (SELECT VALUE payload FROM ONLY $contact_intent_id) != $contact_intent_payload { THROW 'publication:accepted_contact_changed'; };\n");
        if let Some(intent) = &self.intent {
            intent.contact.append_guard_sql(sql, bindings)?;
        }
        Ok(())
    }
}

pub fn validate_accepted_operation(
    space: &str,
    publication: &PublicationFence,
    operation: &StoredOperation,
) -> StorageResult<()> {
    let receipt = &operation.receipt;
    if receipt.space_id != space
        || receipt.source_id != publication.source_id
        || receipt.source_generation != publication.generation
        || receipt.operation_id != publication.operation_id
        || receipt.desired_revision.as_deref() != Some(publication.revision_id.as_str())
        || matches!(
            receipt.state,
            OperationState::Retracted | OperationState::Erasing | OperationState::Erased
        )
        || operation
            .contact_digest
            .as_ref()
            .is_some_and(String::is_empty)
    {
        return Err(conflict("contact_acceptance_identity_changed"));
    }
    Ok(())
}

pub fn check_admission_tx(
    tx: &mut EmbeddedMutation<'_>,
    contact: &PreparedSourceContact,
) -> StorageResult<()> {
    if crate::canonical_commit::epoch_tx(tx, contact.space_id())?.as_deref()
        != Some(contact.observed_epoch())
    {
        return Err(conflict("contact_admission_inputs_changed"));
    }
    contact.check_tx(tx)?;
    tx.advance_canonical_epoch(contact.space_id())?;
    Ok(())
}

pub fn append_admission_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    contact: &PreparedSourceContact,
) -> StorageResult<()> {
    bindings.extend(crate::bindings! {
        "contact_epoch_id" => control_thing("canonical_epoch", contact.space_id()),
        "contact_observed_epoch" => contact.observed_epoch(),
    });
    sql.push_str("IF (SELECT VALUE version FROM ONLY publication_schema:source_contact) != 1 { THROW 'publication:contact_schema_missing'; };\nIF (SELECT VALUE epoch FROM ONLY $contact_epoch_id) != $contact_observed_epoch { THROW 'publication:contact_admission_inputs_changed'; };\n");
    contact.append_guard_sql(sql, bindings)?;
    sql.push_str("UPDATE $contact_epoch_id SET epoch=<string>rand::uuid();\n");
    Ok(())
}

impl SourcePublicationRepository {
    /// Atomically accepts the source and its originally authorized contact plan.
    pub async fn accept_with_prepared_contact(
        &self,
        request: AcceptSourceRevision,
        contact: &PreparedSourceContact,
    ) -> StorageResult<OperationReceipt> {
        self.accept_inner(request, None, None, Some(contact), None, None, None)
            .await
    }

    /// Contact acceptance also fences the original processing configuration.
    pub async fn accept_with_prepared_contact_and_configuration(
        &self,
        request: AcceptSourceRevision,
        contact: &PreparedSourceContact,
        configuration: &elephant_types::provider_binding::SpaceConfigToken,
    ) -> StorageResult<OperationReceipt> {
        self.accept_inner(
            request,
            None,
            Some(configuration),
            Some(contact),
            None,
            None,
            None,
        )
        .await
    }

    /// Background contact reprocessing retains both the original queue attempt
    /// and captured processing configuration while admitting fresh authority.
    pub async fn accept_with_prepared_contact_attempt_and_configuration(
        &self,
        request: AcceptSourceRevision,
        contact: &PreparedSourceContact,
        attempt: &elephant_types::publication::AttemptFence,
        configuration: &elephant_types::provider_binding::SpaceConfigToken,
    ) -> StorageResult<OperationReceipt> {
        self.accept_inner(
            request,
            Some(attempt),
            Some(configuration),
            Some(contact),
            None,
            None,
            None,
        )
        .await
    }

    /// Reads only the accepted capability for this exact current publication.
    ///
    /// It cannot infer contact authority from editable Source metadata or move
    /// a caller's original source fence to a newer head. Fresh acceptance still
    /// prepares and authorizes a new contact plan rather than reusing this one.
    pub async fn accepted_contact_for_publication(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<Option<PreparedSourceContact>> {
        let head = self
            .head(space, &publication.source_id)
            .await?
            .ok_or_else(|| conflict("source_head_missing"))?;
        crate::canonical_commit::check_source_fence(&head, publication)?;
        let state = AcceptedContactState::load(&self.client, space, publication).await?;
        Ok(state.intent.map(|intent| intent.contact))
    }

    /// Reads original contact admission independently of historical plan availability.
    pub async fn accepted_contact_capability_for_publication(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<Option<AcceptedContactCapability>> {
        let head = self
            .head(space, &publication.source_id)
            .await?
            .ok_or_else(|| conflict("source_head_missing"))?;
        crate::canonical_commit::check_source_fence(&head, publication)?;
        self.contact_capability_for_acceptance(space, publication)
            .await
    }

    pub(crate) async fn contact_capability_for_acceptance(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<Option<AcceptedContactCapability>> {
        let payload = self
            .read_control_payload(OPERATION_TABLE, &publication.operation_id, space)
            .await?
            .ok_or_else(|| integrity("accepted contact operation missing"))?;
        let raw: serde_json::Value = crate::source_publication::decode_payload(&payload)?;
        if raw.get("contact_digest").is_none() {
            return Err(conflict("contact_admission_unknown"));
        }
        let operation: StoredOperation = crate::source_publication::decode_payload(&payload)?;
        validate_accepted_operation(space, publication, &operation)?;
        Ok(operation
            .contact_digest
            .map(|digest| AcceptedContactCapability {
                space_id: space.to_owned(),
                publication: publication.clone(),
                digest,
            }))
    }

    /// Read-only semantic retry. The adapter authorizes the original operation
    /// envelope; this method neither recreates a missing plan nor schedules work.
    pub async fn matching_source_receipt(
        &self,
        request: &AcceptSourceRevision,
    ) -> StorageResult<Option<OperationReceipt>> {
        let Some(operation) = self
            .read_control::<StoredOperation>(
                OPERATION_TABLE,
                &crate::source_publication::operation_key(
                    &request.source.base.space_id,
                    &request.idempotency_key,
                ),
                &request.source.base.space_id,
            )
            .await?
        else {
            return Ok(None);
        };
        if operation.request_fingerprint != request.request_fingerprint
            || operation.input_fingerprint
                != input_fingerprint(request, &acceptance_identity_key(request))?
        {
            return Err(conflict("idempotency_conflict"));
        }
        Ok(Some(operation.receipt))
    }

    /// Returns original authority on an identical retry before any new selector
    /// reads.
    ///
    /// The caller must authorize these original old/proposed envelopes.
    /// Erased payload never reappears through this private attachment API.
    pub async fn prepared_contact_retry(
        &self,
        request: &AcceptSourceRevision,
    ) -> StorageResult<Option<PreparedSourceContact>> {
        match self.source_acceptance_retry(request).await? {
            SourceAcceptanceRetry::NewIntent => Ok(None),
            SourceAcceptanceRetry::Existing { contact, .. } => Ok(contact),
            SourceAcceptanceRetry::ExistingContactPlanUnavailable { .. } => {
                Err(conflict("accepted_contact_plan_unavailable"))
            }
        }
    }

    /// Distinguishes a new intent from a matching plain or sealed admission.
    ///
    /// This reads the original operation before checking any newer source head.
    /// Callers still authorize the original returned contact envelopes.
    pub async fn source_acceptance_retry(
        &self,
        request: &AcceptSourceRevision,
    ) -> StorageResult<SourceAcceptanceRetry> {
        let space = &request.source.base.space_id;
        let operation_id =
            crate::source_publication::operation_key(space, &request.idempotency_key);
        let Some(operation) = self
            .read_control::<StoredOperation>(OPERATION_TABLE, &operation_id, space)
            .await?
        else {
            return Ok(SourceAcceptanceRetry::NewIntent);
        };
        // Read-only proof compares the original semantic admission. Producer
        // class and capability presence are returned from that admission, never
        // requested by this lookup; actual acceptance still checks both.
        if operation.request_fingerprint != request.request_fingerprint
            || operation.input_fingerprint
                != input_fingerprint(request, &acceptance_identity_key(request))?
        {
            return Err(conflict("idempotency_conflict"));
        }
        let head = self
            .head(space, &operation.receipt.source_id)
            .await?
            .ok_or_else(|| conflict("source_head_missing"))?;
        if matches!(
            head.lifecycle,
            SourceVisibility::Erased | SourceVisibility::Erasing
        ) {
            return Err(conflict("source_erased"));
        }
        let publication = PublicationFence {
            source_id: operation.receipt.source_id.clone(),
            generation: operation.receipt.source_generation.clone(),
            revision_id: operation
                .receipt
                .desired_revision
                .clone()
                .ok_or_else(|| integrity("accepted revision missing"))?,
            operation_id,
        };
        let state = AcceptedContactState::observe(&self.client, space, &publication).await?;
        if let Some(digest) = state
            .operation
            .contact_digest
            .as_ref()
            .filter(|_| state.intent.is_none())
        {
            return Ok(SourceAcceptanceRetry::ExistingContactPlanUnavailable {
                operation: state.operation.receipt.clone(),
                capability: AcceptedContactCapability {
                    space_id: space.clone(),
                    publication,
                    digest: digest.clone(),
                },
            });
        }
        Ok(SourceAcceptanceRetry::Existing {
            operation: operation.receipt,
            contact: state.intent.map(|intent| intent.contact),
        })
    }

    /// Prepares retained-document reprocessing from its immutable saved input.
    ///
    /// An existing new operation returns its original capability before the old
    /// document fence is checked: successful acceptance has already advanced it.
    /// A new intent must prove that exact old publication is still current.
    pub async fn prepared_reprocess_contact(
        &self,
        request: &AcceptSourceRevision,
        original: &PublicationFence,
    ) -> StorageResult<Option<PreparedSourceContact>> {
        if original.source_id != request.source.base.id {
            return Err(conflict("reprocess_source_incarnation_changed"));
        }
        match self.source_acceptance_retry(request).await? {
            SourceAcceptanceRetry::Existing { contact, .. } => Ok(contact),
            SourceAcceptanceRetry::ExistingContactPlanUnavailable { .. } => {
                Err(conflict("accepted_contact_plan_unavailable"))
            }
            SourceAcceptanceRetry::NewIntent => {
                if self
                    .accepted_contact_capability_for_publication(
                        &request.source.base.space_id,
                        original,
                    )
                    .await?
                    .is_none()
                {
                    return Ok(None);
                }
                PreparedSourceContact::prepare(&self.client, &request.source).await
            }
        }
    }
}

impl CanonicalRepository {
    /// Seeds the original authorized contact contribution before graph promotion.
    /// This never reparses mutable source metadata or refreshes caller authority.
    pub async fn append_accepted_source_effects(
        &self,
        plan: &mut CanonicalPlan,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<()> {
        let repo = SourcePublicationRepository::new(self.client.clone());
        let head = repo
            .head(space, &publication.source_id)
            .await?
            .ok_or_else(|| conflict("source_head_missing"))?;
        drop(repo);
        crate::canonical_commit::check_source_fence(&head, publication)?;
        let state = AcceptedContactState::load(&self.client, space, publication).await?;
        if let Some(intent) = &state.intent {
            intent.contact.append_plan(plan)?;
        }
        Ok(())
    }

    /// Checks the retained structured-contact proof for an exact current Rel.
    ///
    /// Maintenance callers must capture their epoch before reading this proof,
    /// and include the Rel snapshot and published source fence in their commit.
    pub async fn accepted_contact_rel_support(
        &self,
        space: &str,
        rel: &Rel,
        source_id: &RecordId,
    ) -> StorageResult<bool> {
        if rel.base.space_id != space
            || rel.base.deleted_at.is_some()
            || !rel.base.provenance.sources.contains(source_id)
        {
            return Ok(false);
        }
        let repo = SourcePublicationRepository::new(self.client.clone());
        let Some(head) = repo.head(space, source_id).await? else {
            return Ok(false);
        };
        if head.lifecycle != SourceVisibility::Active {
            return Ok(false);
        }
        let Some(revision_id) = head.published_revision else {
            return Ok(false);
        };
        let Some(source) = CanonicalRecord::load(&self.client, space, source_id).await? else {
            return Ok(false);
        };
        if source.base().deleted_at.is_some() {
            return Ok(false);
        }
        let row = CanonicalRecord::Rel(Box::new(rel.clone()));
        if CanonicalRecord::load(&self.client, space, &rel.base.id)
            .await?
            .as_ref()
            .map(CanonicalRecord::fingerprint)
            .transpose()?
            != Some(row.fingerprint()?)
        {
            return Ok(false);
        }
        let support_key = stable_key(&[
            space,
            &source_id.to_string(),
            &revision_id,
            &rel.base.id.to_string(),
        ]);
        let Some(support) = repo
            .read_control::<SourceEffectSupport>("source_effect_support", &support_key, space)
            .await?
        else {
            return Ok(false);
        };
        drop(repo);
        if support.space_id != space
            || support.source_id != *source_id
            || support.source_revision != revision_id
            || support.record_id != rel.base.id
            || support.record_fingerprint != row.fingerprint()?
        {
            return Ok(false);
        }
        crate::source_contact_lineage::proves_rel(
            self,
            space,
            rel,
            source_id,
            &revision_id,
            &support,
        )
        .await
    }

    pub(crate) async fn sealed_contact_publication_rel_support(
        &self,
        space: &str,
        publication: &PublicationFence,
        rel: &Rel,
        receipt: &crate::canonical_commit::CanonicalCommitReceipt,
    ) -> StorageResult<bool> {
        let state = AcceptedContactState::load(&self.client, space, publication).await?;
        if state.operation.receipt.state != OperationState::Completed {
            return Ok(false);
        }
        let Some(intent) = state.intent else {
            return Ok(false);
        };
        if !intent.contact.verifies_rel(rel)? {
            return Ok(false);
        }
        let Some(completion) =
            crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                self.client.clone(),
            )
            .completion(space, publication)
            .await?
        else {
            return Ok(false);
        };
        Ok(completion.space_id == space
            && completion.publication == *publication
            && completion.commit_id == receipt.commit_id
            && receipt.space_id == space
            && receipt.publication_authority.as_ref() == Some(publication)
            && receipt.effect_ids.contains(&rel.base.id))
    }
}

#[cfg(test)]
#[path = "source_contact_publication_tests.rs"]
mod tests;
