//! Read projections and captured source authority for explicit owner closure.
use super::{
    Event, ReadAuthorization, ReadEligibility, Repository, SourceRepository, SourceVisibility,
    StorageResult,
};
use crate::action_disposition::ActionDispositionRepository;
use crate::canonical_commit::{CanonicalRecord, PublishedSourceFence};
use crate::source_publication::{conflict, integrity};

impl ReadEligibility {
    pub(crate) async fn action_source_fences(
        &self,
        space: &str,
    ) -> StorageResult<(Vec<PublishedSourceFence>, Vec<CanonicalRecord>)> {
        let observed = self
            .observed_sources
            .lock()
            .map_err(|_| conflict("action source fence unavailable"))?
            .clone();
        if observed.len() > 8192 {
            return Err(integrity("action source closure too large"));
        }
        let mut fences = Vec::new();
        let mut rows = Vec::new();
        for ((observed_space, id), base) in observed {
            if observed_space != space {
                return Err(integrity("foreign action dependency"));
            }
            let id = id
                .parse()
                .map_err(|_| integrity("action source identity invalid"))?;
            let source = SourceRepository::new(self.storage.clone())
                .get(space, &id)
                .await?
                .ok_or_else(|| conflict("action source absent"))?;
            if source.base != base {
                return Err(conflict("action source changed"));
            }
            match self.head(space, &id).await? {
                Some(head) if head.lifecycle == SourceVisibility::Active => {
                    let revision = head
                        .published_revision
                        .ok_or_else(|| conflict("action source has no current publication"))?;
                    fences.push(PublishedSourceFence::Head {
                        source_id: id,
                        generation: head.generation,
                        published_revision: revision,
                    });
                }
                None => fences.push(PublishedSourceFence::Legacy {
                    source: Box::new(source.clone()),
                }),
                // Retired lineage is readable via its historical proof, but
                // is not current canonical mutation authority.
                Some(_) => return Err(conflict("action depends on retired source authority")),
            }
            rows.push(CanonicalRecord::Source(Box::new(source)));
        }
        Ok((fences, rows))
    }

    pub(super) async fn project_action_dispositions(
        &self,
        event: &mut Event,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<()> {
        // No stored metadata can impersonate this verified response field.
        if let Some(metadata) = event.metadata.as_object_mut() {
            metadata.remove("recall_action_dispositions");
        }
        self.observe_snapshot_epoch(&event.base.space_id).await?;
        let mut dispositions = Vec::new();
        if event.assertion_refs.len() > 8192 {
            return Err(integrity("event action reference bound exceeded"));
        }
        for link in &event.assertion_refs {
            if let Some(disposition) = ActionDispositionRepository::new(self.storage.clone())
                .current(&event.base.space_id, link.id(), authorize)
                .await?
            {
                self.observe_snapshot_epoch(&event.base.space_id).await?;
                dispositions.push(disposition);
            }
        }
        if !dispositions.is_empty() {
            if !event.metadata.is_object() {
                let original = std::mem::replace(&mut event.metadata, serde_json::json!({}));
                event.metadata = serde_json::json!({"recall_original_metadata":original});
            }
            let metadata = event
                .metadata
                .as_object_mut()
                .ok_or_else(|| integrity("action projection metadata wrapper invalid"))?;
            metadata.insert(
                "recall_action_dispositions".into(),
                serde_json::to_value(dispositions)
                    .map_err(|_| integrity("action projection encoding"))?,
            );
        }
        Ok(())
    }
}
