//! Space-local processor authority. These records contain references, never secrets.
//!
//! A resolved binding is a description, not a durable dispatch permit. Every
//! transport request (including retry and agent continuation) must reload it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Opt-in asymmetric English BGE retrieval recipe; existing profiles stay unchanged.
pub const BGE_EN_RETRIEVAL_V1: &str = "bge-en-retrieval-v1";

/// Exact BAAI query instruction, including its trailing space. Documents stay raw.
pub const BGE_EN_RETRIEVAL_QUERY_INSTRUCTION: &str =
    "Represent this sentence for searching relevant passages: ";

/// Model/dimension combinations covered by the versioned English retrieval recipe.
#[must_use]
pub fn bge_en_retrieval_dimensions(model: &str) -> Option<usize> {
    match model {
        "bge-small-en-v1.5" | "bge-small-en-v1.5-q" => Some(384),
        "bge-base-en-v1.5" | "bge-base-en-v1.5-q" => Some(768),
        "bge-large-en-v1.5" | "bge-large-en-v1.5-q" => Some(1024),
        _ => None,
    }
}

/// Outbound operations have independent approvals; ingestion grants cannot add them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelOperation {
    /// Embed a retrieval query against the active vector profile.
    QueryEmbedding,
    /// Embed ingested document content with the active vector profile.
    DocumentEmbedding,
    /// Embed existing content into the approved pending replacement partition.
    ReindexEmbedding,
    /// Extract candidate knowledge from source content.
    Extraction,
    /// Select the processing treatment for accepted source content.
    Triage,
    /// Check whether cited source evidence supports proposed knowledge.
    SupportVerification,
    /// Generate a summary of authorized content.
    Summary,
    /// Resolve candidate identities while planning knowledge promotion.
    PromotionDisambiguation,
    /// Evaluate derived knowledge during truth maintenance.
    TruthMaintenance,
    /// Generate or revise a knowledge-view document.
    WikiWriter,
    /// Verify generated knowledge-view content against its evidence.
    WikiVerifier,
    /// Plan knowledge-view generation or maintenance.
    WikiPlanner,
    /// Render media into content used by the document pipeline.
    MediaRender,
    /// Resolve an identity ambiguity through an approved model.
    IdentityDisambiguation,
}

impl ModelOperation {
    /// Whether this operation requires an embedding profile and vector dimensions.
    #[must_use]
    pub const fn is_embedding(self) -> bool {
        matches!(
            self,
            Self::QueryEmbedding | Self::DocumentEmbedding | Self::ReindexEmbedding
        )
    }
}

/// The account and endpoint an administrator approved for a space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConnection {
    /// Stable connection identifier, equal to its key in the space connection registry.
    pub id: String,
    /// Transport provider identifier: `openai`, `anthropic`, `gemini`, or `fastembed`.
    pub provider: String,
    /// Approved account identity included in vector partition isolation.
    pub account_id: String,
    /// Approved remote base URL, or `file:///absolute/assets-root` for pinned `FastEmbed`.
    /// Credentials in URL userinfo, query parameters, or fragments are prohibited.
    pub endpoint: String,
    /// Name in the deployment secret resolver, never a credential value.
    pub credential_ref: Option<String>,
    /// Incremented on rotation; rotation does not change vector identity.
    pub credential_generation: u64,
    /// Whether every binding that uses this connection is denied.
    pub revoked: bool,
}

/// Immutable model identity; changes get a new profile ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProfile {
    /// Immutable profile identifier, equal to its key in the space profile registry.
    pub id: String,
    /// Identifier of the approved connection that supplies this model.
    pub connection_id: String,
    /// Provider model name; local assets must declare this exact model identity.
    pub model: String,
    /// Provider's immutable revision, or an operator-pinned deployment version.
    /// `FastEmbed` uses `sha256:<manifest digest>` to bind verified local asset bytes;
    /// the asset manifest must match this profile's model, dimensions, and preprocessing.
    pub model_version: String,
    /// Required for embeddings; absent for generative profiles.
    pub dimensions: Option<usize>,
    /// Explicit preprocessing contract. Embeddings support `text-v1` and, on
    /// `OpenAI`/`FastEmbed`, `text-full-v2`. English BGE `FastEmbed` models also support
    /// `bge-en-retrieval-v1`; changing preprocessing requires a new profile.
    pub preprocessing: String,
    /// Optional immutable policy for a summary-only generative profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<crate::summary_policy::SummaryPolicy>,
}

impl ModelProfile {
    /// Validate preprocessing against its provider, model and vector dimensions.
    ///
    /// # Errors
    /// Rejects unsupported embedding recipes and embedding-only generative recipes.
    pub fn validate_preprocessing(&self, provider: &str) -> Result<(), ProviderAuthorizationError> {
        let invalid =
            |message: &str| ProviderAuthorizationError::InvalidConfiguration(message.into());
        if let Some(policy) = &self.summary {
            if self.dimensions.is_some() {
                return Err(invalid("summary policy requires a generative profile"));
            }
            policy.validate(provider, &self.model)?;
        }
        if let Some(dimensions) = self.dimensions {
            let supported = match self.preprocessing.as_str() {
                "text-v1" => true,
                "text-full-v2" => matches!(provider, "openai" | "fastembed"),
                BGE_EN_RETRIEVAL_V1 => {
                    provider == "fastembed"
                        && bge_en_retrieval_dimensions(&self.model) == Some(dimensions)
                }
                _ => false,
            };
            if !supported {
                return Err(invalid(
                    "unsupported embedding preprocessing for provider/model",
                ));
            }
        } else if self.preprocessing == "text-full-v2" {
            return Err(invalid(
                "full-input preprocessing requires an embedding profile",
            ));
        } else if self.preprocessing == BGE_EN_RETRIEVAL_V1 {
            return Err(invalid(
                "BGE retrieval preprocessing requires an embedding profile",
            ));
        }
        Ok(())
    }
}

/// Explicit approval for one operation and profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationBinding {
    /// Approval identifier retained in model-call provenance.
    pub id: String,
    /// Immutable profile approved for the operation that keys this binding.
    pub profile_id: String,
    /// Whether dispatch through this operation binding is denied.
    pub revoked: bool,
}

/// Durable scan phase: document nodes must finish before entity coverage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingReindexPhase {
    /// Scan and embed document nodes in stable cursor order.
    #[default]
    Nodes,
    /// Scan and embed entities after document-node coverage is complete.
    Entities,
}

/// A staged replacement partition. Activation requires durable full coverage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingReindex {
    /// Durable identifier of this staged reindex operation.
    pub id: String,
    /// Previously active profile, or `None` when establishing the first partition.
    pub from_profile_id: Option<String>,
    /// Approved destination profile whose partition is being populated.
    pub to_profile_id: String,
    /// Space content generation captured by the reindex owner.
    pub content_generation: u64,
    /// Last processed record cursor within the current scan phase.
    pub cursor: Option<String>,
    /// Number of document nodes recorded as embedded by this reindex.
    pub embedded_nodes: u64,
    /// Number of entities recorded as embedded by this reindex.
    #[serde(default)]
    pub embedded_entities: u64,
    /// Current coverage scan phase; entity scanning follows document nodes.
    #[serde(default)]
    pub phase: EmbeddingReindexPhase,
    /// Whether the staged scan finished; activation still requires checked coverage.
    pub complete: bool,
}

/// Repository-owned evidence of a completed scan and guarded activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedEmbeddingReindex {
    /// Exact completed reindex operation identity.
    pub reindex_id: String,
    /// Indexed-content generation covered by the completed scan.
    pub content_generation: u64,
    /// Configuration generation written by activation.
    pub activated_config_generation: u64,
    /// Complete activated vector identity.
    pub partition: VectorPartition,
    /// Actual activation time, distinct from historical import time.
    pub activated_at: chrono::DateTime<chrono::Utc>,
}

/// Single per-space authority owned by the space config repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpaceProcessingConfig {
    /// Monotonic configuration revision used to reject stale administrative writes.
    pub generation: u64,
    /// Storage-owned epoch, advanced atomically by every indexed-content mutation.
    #[serde(default)]
    pub index_generation: u64,
    /// Explicit processor policy identity captured by jobs and provenance.
    pub policy_id: String,
    /// Approved connections indexed by their stable identifiers.
    pub connections: BTreeMap<String, ProviderConnection>,
    /// Immutable model profiles indexed by their identifiers; changes use a new ID.
    pub profiles: BTreeMap<String, ModelProfile>,
    /// Explicit operation approvals; an absent entry does not permit dispatch.
    pub bindings: BTreeMap<ModelOperation, OperationBinding>,
    /// Profile used for ordinary query and document embedding, if activated.
    pub active_embedding_profile_id: Option<String>,
    /// Storage-owned progress for a staged replacement vector partition, if any.
    pub pending_reindex: Option<EmbeddingReindex>,
    /// Repository-owned last activation evidence; ordinary writes must preserve it.
    #[serde(default)]
    pub completed_embedding_reindex: Option<CompletedEmbeddingReindex>,
}

/// Safe snapshot used by ingestion, discovery and publication preconditions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpaceConfigToken {
    /// Named space whose processing authority was observed.
    pub space_id: String,
    /// Persistent space configuration record identifier.
    pub config_id: String,
    /// Observed processing configuration revision.
    pub generation: u64,
    /// Observed processor policy identity.
    pub policy_id: String,
}

/// A resolved space always exists and carries explicit processor policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSpaceConfig {
    /// Observed authority identifiers used to fence work and record provenance.
    pub token: SpaceConfigToken,
    /// Resolved processor approvals that must be rechecked before dispatch.
    pub processing: SpaceProcessingConfig,
    /// Optional purpose text supplied by the space configuration.
    pub purpose: Option<String>,
    /// Optional processing skill text supplied by the space configuration.
    pub skill: Option<String>,
}

/// Safe provenance for a model call. No endpoint, secret reference or secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelBindingProvenance {
    /// Space and configuration revision authorizing the observed binding.
    pub config: SpaceConfigToken,
    /// Specific model operation requested under this approval.
    pub operation: ModelOperation,
    /// Operation approval identifier selected during resolution.
    pub binding_id: String,
    /// Immutable model profile selected during resolution.
    pub profile_id: String,
}

/// Resolved transport settings. Recheck current authority at every request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedModelBinding {
    /// Payload-free authority references suitable for recording the model call.
    pub provenance: ModelBindingProvenance,
    /// Approved transport settings and secret reference, without a secret value.
    pub connection: ProviderConnection,
    /// Immutable model identity and preprocessing settings for this call.
    pub profile: ModelProfile,
}

/// Vector/cache identity includes the space and every semantic transport choice.
/// Credential rotation deliberately leaves this identity unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VectorPartition {
    /// Named space that owns the vectors.
    pub space_id: String,
    /// Provider implementation that produced the vectors.
    pub provider: String,
    /// Approved provider account identity.
    pub account_id: String,
    /// Approved remote endpoint or local pinned-asset root.
    pub endpoint: String,
    /// Immutable embedding profile identifier.
    pub profile_id: String,
    /// Exact provider model name.
    pub model: String,
    /// Immutable remote version or local `sha256:<manifest digest>` asset pin.
    pub model_version: String,
    /// Required vector length for this partition.
    pub dimensions: usize,
    /// Versioned text preparation contract used before embedding.
    pub preprocessing: String,
}

/// A processing authority or configuration failure that prevents model dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProviderAuthorizationError {
    /// The requested named space has no configuration record.
    #[error("space configuration is missing")]
    MissingSpace,
    /// The space has no explicit approved processing configuration.
    #[error("space has no approved processing configuration")]
    MissingConfiguration,
    /// The requested operation, referenced profile, or connection is absent.
    #[error("model operation has no approved binding")]
    MissingBinding,
    /// The selected operation binding or its connection was revoked.
    #[error("model binding or connection is revoked")]
    Revoked,
    /// The selected profile differs from pinned or currently active authority.
    #[error("model profile is not the currently approved profile")]
    ProfileMismatch,
    /// The request attempts to use authority resolved for another space.
    #[error("model operation belongs to a different space")]
    WrongSpace,
    /// Stored processing metadata violates an identity or structural constraint.
    #[error("invalid processing configuration: {0}")]
    InvalidConfiguration(String),
    /// An embedding profile change needs completed reindex coverage and activation.
    #[error("embedding profile change requires a completed reindex and explicit activation")]
    ReindexRequired,
}

impl ResolvedSpaceConfig {
    /// Resolves current policy, optionally requiring an immutable job profile.
    ///
    /// # Errors
    /// Missing, revoked, wrong-space or stale-profile authority fails closed.
    pub fn authorize(
        &self,
        space_id: &str,
        operation: ModelOperation,
        pinned_profile: Option<&str>,
    ) -> Result<ResolvedModelBinding, ProviderAuthorizationError> {
        if self.token.space_id != space_id {
            return Err(ProviderAuthorizationError::WrongSpace);
        }
        self.processing.validate()?;
        let binding = self
            .processing
            .bindings
            .get(&operation)
            .ok_or(ProviderAuthorizationError::MissingBinding)?;
        let profile = self
            .processing
            .profiles
            .get(&binding.profile_id)
            .ok_or(ProviderAuthorizationError::MissingBinding)?;
        let connection = self
            .processing
            .connections
            .get(&profile.connection_id)
            .ok_or(ProviderAuthorizationError::MissingBinding)?;
        if binding.revoked || connection.revoked {
            return Err(ProviderAuthorizationError::Revoked);
        }
        if pinned_profile.is_some_and(|pinned| pinned != profile.id)
            || (operation.is_embedding()
                && if operation == ModelOperation::ReindexEmbedding {
                    self.processing
                        .pending_reindex
                        .as_ref()
                        .map(|pending| pending.to_profile_id.as_str())
                        != Some(profile.id.as_str())
                } else {
                    self.processing.active_embedding_profile_id.as_deref()
                        != Some(profile.id.as_str())
                })
        {
            return Err(ProviderAuthorizationError::ProfileMismatch);
        }
        if operation.is_embedding() != profile.dimensions.is_some() {
            return Err(ProviderAuthorizationError::InvalidConfiguration(
                "profile kind does not match operation".into(),
            ));
        }
        Ok(ResolvedModelBinding {
            provenance: ModelBindingProvenance {
                config: self.token.clone(),
                operation,
                binding_id: binding.id.clone(),
                profile_id: profile.id.clone(),
            },
            connection: connection.clone(),
            profile: profile.clone(),
        })
    }
}

impl ResolvedModelBinding {
    /// Derives the vector partition identity without reading credentials or making a request.
    ///
    /// # Errors
    /// Generative model profiles cannot address vector partitions.
    pub fn vector_partition(&self) -> Result<VectorPartition, ProviderAuthorizationError> {
        let dimensions = self
            .profile
            .dimensions
            .ok_or(ProviderAuthorizationError::ProfileMismatch)?;
        Ok(VectorPartition {
            space_id: self.provenance.config.space_id.clone(),
            provider: self.connection.provider.clone(),
            account_id: self.connection.account_id.clone(),
            endpoint: self.connection.endpoint.clone(),
            profile_id: self.profile.id.clone(),
            model: self.profile.model.clone(),
            model_version: self.profile.model_version.clone(),
            dimensions,
            preprocessing: self.profile.preprocessing.clone(),
        })
    }
}

impl SpaceProcessingConfig {
    /// Validates stored authority without consulting credentials or the network.
    ///
    /// # Errors
    /// Rejects malformed identities, dangling references and fake processors.
    pub fn validate(&self) -> Result<(), ProviderAuthorizationError> {
        let invalid =
            |message: &str| ProviderAuthorizationError::InvalidConfiguration(message.into());
        if self.generation == 0 || self.policy_id.trim().is_empty() {
            return Err(invalid("generation and policy_id are required"));
        }
        for (id, connection) in &self.connections {
            if id.is_empty()
                || id != &connection.id
                || connection.account_id.trim().is_empty()
                || connection.credential_generation == 0
            {
                return Err(invalid("connection identity is incomplete"));
            }
            if !matches!(
                connection.provider.as_str(),
                "openai" | "anthropic" | "gemini" | "fastembed"
            ) {
                return Err(invalid("unsupported or synthetic provider"));
            }
            if connection.provider != "fastembed"
                && (connection.endpoint.is_empty()
                    || connection
                        .credential_ref
                        .as_ref()
                        .is_none_or(|name| name.trim().is_empty()))
            {
                return Err(invalid(
                    "remote connection requires endpoint and credential reference",
                ));
            }
        }
        for (id, profile) in &self.profiles {
            if id.is_empty()
                || id != &profile.id
                || profile.model.trim().is_empty()
                || profile.model_version.trim().is_empty()
                || profile.preprocessing.trim().is_empty()
                || profile.dimensions == Some(0)
                || !self.connections.contains_key(&profile.connection_id)
            {
                return Err(invalid("model profile is incomplete"));
            }
            let connection = self
                .connections
                .get(&profile.connection_id)
                .ok_or_else(|| invalid("model profile has no connection"))?;
            profile.validate_preprocessing(&connection.provider)?;
        }
        for (operation, binding) in &self.bindings {
            if self
                .profiles
                .get(&binding.profile_id)
                .is_some_and(|profile| profile.summary.is_some())
                && *operation != ModelOperation::Summary
            {
                return Err(invalid("summary policy requires the summary operation"));
            }
            if binding.id.is_empty() || !self.profiles.contains_key(&binding.profile_id) {
                return Err(invalid("binding has no profile"));
            }
        }
        if let Some(profile) = &self.active_embedding_profile_id {
            if self
                .profiles
                .get(profile)
                .is_none_or(|p| p.dimensions.is_none())
            {
                return Err(invalid("active embedding profile is missing or non-vector"));
            }
        }
        Ok(())
    }

    /// Validate an ordinary admin replacement. Activation uses a separate
    /// repository transaction with a repository-owned coverage proof.
    ///
    /// # Errors
    /// Generation conflicts, in-place profile changes and implicit activation fail.
    pub fn validate_replacement(&self, next: &Self) -> Result<(), ProviderAuthorizationError> {
        next.validate()?;
        if self.generation.checked_add(1) != Some(next.generation) {
            return Err(ProviderAuthorizationError::InvalidConfiguration(
                "config generation conflict".into(),
            ));
        }
        for (id, profile) in &self.profiles {
            if next.profiles.get(id) != Some(profile) {
                return Err(ProviderAuthorizationError::InvalidConfiguration(
                    "profile identities are immutable".into(),
                ));
            }
        }
        for (id, connection) in &self.connections {
            if let Some(next) = next.connections.get(id) {
                if connection.provider != next.provider
                    || connection.account_id != next.account_id
                    || connection.endpoint != next.endpoint
                {
                    return Err(ProviderAuthorizationError::InvalidConfiguration(
                        "connection identity is immutable; create a new connection".into(),
                    ));
                }
                if next.credential_generation < connection.credential_generation
                    || (next.credential_ref != connection.credential_ref
                        && next.credential_generation <= connection.credential_generation)
                {
                    return Err(ProviderAuthorizationError::InvalidConfiguration(
                        "credential rotation requires a newer generation".into(),
                    ));
                }
            }
        }
        if self.index_generation != next.index_generation
            || self.pending_reindex != next.pending_reindex
            || self.completed_embedding_reindex != next.completed_embedding_reindex
        {
            return Err(ProviderAuthorizationError::InvalidConfiguration(
                "reindex progress and content epoch are storage-owned".into(),
            ));
        }
        if self.active_embedding_profile_id != next.active_embedding_profile_id {
            return Err(ProviderAuthorizationError::ReindexRequired);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn space(id: &str) -> ResolvedSpaceConfig {
        let connection = ProviderConnection {
            id: "conn".into(),
            provider: "openai".into(),
            account_id: "account".into(),
            endpoint: "https://example.test/v1".into(),
            credential_ref: Some("RECALL_TEST_KEY".into()),
            credential_generation: 1,
            revoked: false,
        };
        let profile = ModelProfile {
            id: "embed-v1".into(),
            connection_id: "conn".into(),
            model: "embed".into(),
            model_version: "v1".into(),
            dimensions: Some(3),
            preprocessing: "text-v1".into(),
            summary: None,
        };
        let processing = SpaceProcessingConfig {
            generation: 1,
            index_generation: 0,
            policy_id: "policy".into(),
            connections: BTreeMap::from([("conn".into(), connection)]),
            profiles: BTreeMap::from([("embed-v1".into(), profile)]),
            bindings: BTreeMap::from([(
                ModelOperation::QueryEmbedding,
                OperationBinding {
                    id: "query".into(),
                    profile_id: "embed-v1".into(),
                    revoked: false,
                },
            )]),
            active_embedding_profile_id: Some("embed-v1".into()),
            pending_reindex: None,
            completed_embedding_reindex: None,
        };
        ResolvedSpaceConfig {
            token: SpaceConfigToken {
                space_id: id.into(),
                config_id: "config".into(),
                generation: 1,
                policy_id: "policy".into(),
            },
            processing,
            purpose: None,
            skill: None,
        }
    }

    #[test]
    fn named_spaces_and_current_revocation_fail_closed() {
        let mut a = space("a");
        assert_eq!(
            a.authorize("b", ModelOperation::QueryEmbedding, None),
            Err(ProviderAuthorizationError::WrongSpace)
        );
        assert_eq!(
            a.authorize("a", ModelOperation::Extraction, None),
            Err(ProviderAuthorizationError::MissingBinding)
        );
        assert_eq!(
            a.authorize("a", ModelOperation::QueryEmbedding, Some("old")),
            Err(ProviderAuthorizationError::ProfileMismatch)
        );
        a.processing.connections.get_mut("conn").unwrap().revoked = true;
        assert_eq!(
            a.authorize("a", ModelOperation::QueryEmbedding, Some("embed-v1")),
            Err(ProviderAuthorizationError::Revoked)
        );
    }

    #[test]
    fn partitions_separate_spaces_versions_and_rotation() {
        let a = space("a");
        let mut b = space("b");
        let partition = |s: &ResolvedSpaceConfig| {
            s.authorize(&s.token.space_id, ModelOperation::QueryEmbedding, None)
                .unwrap()
                .vector_partition()
                .unwrap()
        };
        assert_ne!(partition(&a), partition(&b));
        b.token.space_id = "a".into();
        assert_eq!(partition(&a), partition(&b));
        b.processing
            .connections
            .get_mut("conn")
            .unwrap()
            .credential_generation = 2;
        assert_eq!(partition(&a), partition(&b));
        b.processing
            .profiles
            .get_mut("embed-v1")
            .unwrap()
            .model_version = "v2".into();
        assert_ne!(partition(&a), partition(&b));
    }

    #[test]
    fn profile_replacement_and_synthetic_providers_are_rejected() {
        let a = space("a");
        let mut next = a.processing.clone();
        next.generation += 1;
        next.profiles.get_mut("embed-v1").unwrap().model = "different".into();
        assert!(a.processing.validate_replacement(&next).is_err());
        next = a.processing.clone();
        next.connections.get_mut("conn").unwrap().provider = "fake".into();
        assert!(next.validate().is_err());
        let provenance = a
            .authorize("a", ModelOperation::QueryEmbedding, None)
            .unwrap()
            .provenance;
        let wire = serde_json::to_string(&provenance).unwrap();
        assert!(!wire.contains("RECALL_TEST_KEY"));
        assert!(!wire.contains("endpoint"));
    }

    #[test]
    fn full_input_v2_requires_explicit_vector_profile_and_supported_provider() {
        for provider in ["openai", "fastembed", "anthropic", "gemini"] {
            let mut configured = space("a");
            let connection = configured.processing.connections.get_mut("conn").unwrap();
            connection.provider = provider.into();
            if provider == "fastembed" {
                connection.endpoint = "file:///pinned-test-assets".into();
                connection.credential_ref = None;
            }
            let profile = configured.processing.profiles.get_mut("embed-v1").unwrap();
            profile.preprocessing = "text-full-v2".into();
            if provider == "fastembed" {
                profile.model = "all-MiniLM-L6-v2".into();
                profile.model_version = format!("sha256:{}", "0".repeat(64));
                profile.dimensions = Some(384);
            }
            assert_eq!(
                configured.processing.validate().is_ok(),
                matches!(provider, "openai" | "fastembed")
            );
        }
        let mut generative = space("a").processing;
        generative.profiles.get_mut("embed-v1").unwrap().dimensions = None;
        generative
            .profiles
            .get_mut("embed-v1")
            .unwrap()
            .preprocessing = "text-full-v2".into();
        generative.active_embedding_profile_id = None;
        generative.bindings.clear();
        assert!(
            matches!(generative.validate(), Err(ProviderAuthorizationError::InvalidConfiguration(message))
            if message == "full-input preprocessing requires an embedding profile")
        );
        for unsupported in ["text-full-v3", "TEXT-FULL-V2", "text-full-v2 "] {
            let mut configured = space("a").processing;
            configured
                .profiles
                .get_mut("embed-v1")
                .unwrap()
                .preprocessing = unsupported.into();
            assert!(configured.validate().is_err());
        }
    }

    #[test]
    fn bge_retrieval_recipe_checks_provider_model_dimensions_and_profile_kind() {
        let mut profile = space("a").processing.profiles["embed-v1"].clone();
        profile.preprocessing = BGE_EN_RETRIEVAL_V1.into();
        for (model, dimensions) in [
            ("bge-small-en-v1.5", 384),
            ("bge-small-en-v1.5-q", 384),
            ("bge-base-en-v1.5", 768),
            ("bge-base-en-v1.5-q", 768),
            ("bge-large-en-v1.5", 1024),
            ("bge-large-en-v1.5-q", 1024),
        ] {
            profile.model = model.into();
            profile.dimensions = Some(dimensions);
            assert!(profile.validate_preprocessing("fastembed").is_ok());
            for provider in ["openai", "anthropic", "gemini", "fake", "hashed"] {
                assert!(profile.validate_preprocessing(provider).is_err());
            }
            for dimensions in [Some(0), Some(dimensions + 1), None] {
                profile.dimensions = dimensions;
                assert!(profile.validate_preprocessing("fastembed").is_err());
            }
        }
        profile.dimensions = Some(384);
        for model in [
            "all-minilm-l6-v2",
            "bge-small-zh-v1.5",
            "bge-small-en",
            "BGE-SMALL-EN-V1.5",
        ] {
            profile.model = model.into();
            assert!(profile.validate_preprocessing("fastembed").is_err());
        }
        profile.model = "bge-small-en-v1.5".into();
        for version in [
            "bge-en-retrieval-v2",
            "BGE-EN-RETRIEVAL-V1",
            "bge-en-retrieval-v1 ",
        ] {
            profile.preprocessing = version.into();
            assert!(profile.validate_preprocessing("fastembed").is_err());
        }
    }

    #[test]
    fn bge_retrieval_staging_preserves_original_partition_and_requires_reindex() {
        let mut original = space("a");
        let connection = original.processing.connections.get_mut("conn").unwrap();
        connection.provider = "fastembed".into();
        connection.endpoint = "file:///approved-model-assets".into();
        connection.credential_ref = None;
        let profile = original.processing.profiles.get_mut("embed-v1").unwrap();
        profile.model = "bge-small-en-v1.5".into();
        profile.model_version = format!("sha256:{}", "a".repeat(64));
        profile.dimensions = Some(384);
        original.processing.validate().unwrap();
        let before = original
            .authorize("a", ModelOperation::QueryEmbedding, None)
            .unwrap();
        let mut replacement = before.profile.clone();
        replacement.id = "bge-retrieval-new".into();
        replacement.model_version = format!("sha256:{}", "b".repeat(64));
        replacement.preprocessing = BGE_EN_RETRIEVAL_V1.into();
        let mut staged = original.processing.clone();
        staged.generation += 1;
        staged
            .profiles
            .insert(replacement.id.clone(), replacement.clone());
        original.processing.validate_replacement(&staged).unwrap();
        let staged_space = ResolvedSpaceConfig {
            processing: staged.clone(),
            ..original.clone()
        };
        let after = staged_space
            .authorize("a", ModelOperation::QueryEmbedding, None)
            .unwrap();
        assert_eq!(
            before.vector_partition().unwrap(),
            after.vector_partition().unwrap()
        );
        staged.active_embedding_profile_id = Some(replacement.id.clone());
        assert_eq!(
            original.processing.validate_replacement(&staged),
            Err(ProviderAuthorizationError::ReindexRequired)
        );
        staged
            .active_embedding_profile_id
            .clone_from(&original.processing.active_embedding_profile_id);
        staged.profiles.get_mut("embed-v1").unwrap().preprocessing = BGE_EN_RETRIEVAL_V1.into();
        assert!(original.processing.validate_replacement(&staged).is_err());
        let candidate = ResolvedModelBinding {
            profile: replacement,
            ..before.clone()
        };
        assert_ne!(
            before.vector_partition().unwrap(),
            candidate.vector_partition().unwrap()
        );
    }

    #[test]
    fn full_input_profile_preserves_old_identity_and_requires_explicit_activation() {
        let original = space("a");
        let old_profile = original.processing.profiles["embed-v1"].clone();
        let mut full_profile = old_profile.clone();
        full_profile.id = "embed-full-v2".into();
        full_profile.preprocessing = "text-full-v2".into();
        let mut staged = original.processing.clone();
        staged.generation += 1;
        staged
            .profiles
            .insert(full_profile.id.clone(), full_profile.clone());
        original.processing.validate_replacement(&staged).unwrap();
        assert_eq!(staged.profiles["embed-v1"], old_profile);
        let still_active = ResolvedSpaceConfig {
            token: SpaceConfigToken {
                generation: staged.generation,
                ..original.token.clone()
            },
            processing: staged.clone(),
            ..original.clone()
        }
        .authorize("a", ModelOperation::QueryEmbedding, None)
        .unwrap();
        assert_eq!(
            still_active.vector_partition().unwrap().preprocessing,
            "text-v1"
        );

        let mut relabeled = original.processing.clone();
        relabeled.generation += 1;
        relabeled
            .profiles
            .get_mut("embed-v1")
            .unwrap()
            .preprocessing = "text-full-v2".into();
        assert!(original
            .processing
            .validate_replacement(&relabeled)
            .is_err());
        staged.active_embedding_profile_id = Some(full_profile.id.clone());
        assert_eq!(
            original.processing.validate_replacement(&staged),
            Err(ProviderAuthorizationError::ReindexRequired)
        );

        // Pure binding projection after explicit activation; this is not a
        // fabricated coverage receipt or an executed reindex qualification.
        staged
            .bindings
            .get_mut(&ModelOperation::QueryEmbedding)
            .unwrap()
            .profile_id = full_profile.id;
        let resolved = ResolvedSpaceConfig {
            token: SpaceConfigToken {
                generation: staged.generation,
                ..original.token.clone()
            },
            processing: staged,
            ..original.clone()
        }
        .authorize("a", ModelOperation::QueryEmbedding, None)
        .unwrap();
        let full_partition = resolved.vector_partition().unwrap();
        assert_eq!(full_partition.preprocessing, "text-full-v2");
        assert_ne!(full_partition, still_active.vector_partition().unwrap());
        assert_eq!(
            original.processing.profiles["embed-v1"].preprocessing,
            "text-v1"
        );
    }
}
