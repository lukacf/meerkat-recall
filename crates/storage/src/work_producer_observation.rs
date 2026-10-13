//! Bounded native observations; original payloads and commit guards own authority.
use super::{
    changed, invalid, origin_id, BTreeMap, Deserialize, OriginEnvelope, RecordId,
    ReprocessExecutionOrigin, StorageResult, TableName, Thing, Value, WorkItem, WorkProducerOrigin,
    WorkProducerRepository, WorkRun,
};
use crate::work_item_repository::{WorkItemDb, WORK_ITEM_PROJECTION};
use crate::work_run_repository::{WorkRunDb, WORK_RUN_PROJECTION};
use std::collections::BTreeSet;

#[derive(Deserialize)]
struct OriginHintDb {
    id: Thing,
    space_id: String,
    work_item_id: Thing,
    parent_work_item_id: Option<Thing>,
    parent_work_run_id: Option<Thing>,
    payload: String,
}

#[derive(Deserialize)]
struct ChainDb {
    work_ids: Vec<Thing>,
    origins: Vec<OriginHintDb>,
}

pub(super) struct NativeLineageObservation {
    works: BTreeMap<RecordId, WorkItem>,
    runs: BTreeMap<RecordId, WorkRun>,
    origins: BTreeMap<RecordId, OriginEnvelope>,
}

// Anonymous closures have isolated contexts in the pinned SDK. Carry space
// explicitly. Public Work IDs are native UUID Things; private origin IDs are
// String UUID keys. Never construct either from a rendered record expression.
const CHAIN_QUERY: &str = r"
LET $producer_chain = array::fold(array::range(0, 256),
    {space: $space, next: $start_work, ids: [], done: false},
    |$walk, $ordinal| {
        IF $walk.done { RETURN $walk; };
        IF meta::tb($walk.next) != 'work_item' OR $walk.next IN $walk.ids {
            THROW 'work_producer_origin_changed_or_unknown';
        };
        LET $origin_id = type::thing('work_producer_origin', <string>meta::id($walk.next));
        LET $hint = SELECT space_id, work_item_id, parent_work_item_id FROM ONLY $origin_id;
        IF $hint = NONE OR $hint.space_id != $walk.space OR $hint.work_item_id != $walk.next {
            THROW 'work_producer_origin_changed_or_unknown';
        };
        RETURN {
            space: $walk.space,
            next: $hint.parent_work_item_id,
            ids: array::append($walk.ids, $walk.next),
            done: $hint.parent_work_item_id = NONE
        };
    });
IF !$producer_chain.done { THROW 'work_producer_origin_changed_or_unknown'; };
RETURN {
    work_ids: $producer_chain.ids,
    origins: (SELECT id, space_id, work_item_id, parent_work_item_id, parent_work_run_id, payload
        FROM array::map($producer_chain.ids, |$work| type::thing('work_producer_origin', <string>meta::id($work)))
        WHERE space_id = $space)
};
";

fn original_parent(origin: &WorkProducerOrigin) -> StorageResult<Option<(&RecordId, &RecordId)>> {
    match origin {
        WorkProducerOrigin::Direct
        | WorkProducerOrigin::ReprocessJob {
            execution_origin: ReprocessExecutionOrigin::Independent,
            ..
        } => Ok(None),
        WorkProducerOrigin::Child {
            parent_work_item_id,
            original_parent_work_run_id,
            ..
        }
        | WorkProducerOrigin::ReprocessJob {
            execution_origin:
                ReprocessExecutionOrigin::Inherited {
                    parent_work_item_id,
                    original_parent_work_run_id,
                    ..
                },
            ..
        } => {
            if parent_work_item_id.table() != TableName::WorkItem
                || original_parent_work_run_id.table() != TableName::WorkRun
            {
                return Err(changed());
            }
            Ok(Some((parent_work_item_id, original_parent_work_run_id)))
        }
        WorkProducerOrigin::Unknown => Err(changed()),
    }
}

fn record_targets(ids: impl IntoIterator<Item = RecordId>) -> Value {
    Value::Array(surrealdb::sql::Array::from(
        ids.into_iter()
            .map(|id| Value::Thing(crate::surreal::thing_from_record_id(&id)))
            .collect::<Vec<_>>(),
    ))
}

impl NativeLineageObservation {
    #[allow(
        clippy::too_many_lines,
        reason = "Producer observation joins parent work, run, origin, policy, and configuration under one captured epoch; preserve dependent read order and final consistency checks."
    )]
    pub(super) async fn capture(
        repository: &WorkProducerRepository,
        parent: &WorkItem,
    ) -> StorageResult<Self> {
        let space = &parent.base.space_id;
        let mut response = repository
            .client
            .query_bind_checked(
                CHAIN_QUERY,
                crate::bindings! {
                    "space" => space.clone(),
                    "start_work" => crate::surreal::thing_from_record_id(&parent.base.id),
                },
            )
            .await?;
        let last = response
            .num_statements()
            .checked_sub(1)
            .ok_or_else(changed)?;
        let chain: Option<ChainDb> = response.take(last).map_err(invalid)?;
        let chain = chain.ok_or_else(changed)?;
        if chain.work_ids.is_empty()
            || chain.work_ids.len() > 256
            || chain.origins.len() != chain.work_ids.len()
        {
            return Err(changed());
        }
        let mut origins = BTreeMap::new();
        for hint in chain.origins {
            let envelope: OriginEnvelope = serde_json::from_str(&hint.payload).map_err(invalid)?;
            if envelope.version != 1
                || envelope.space_id != *space
                || hint.space_id != *space
                || envelope.work_item_id.table() != TableName::WorkItem
                || hint.work_item_id != crate::surreal::thing_from_record_id(&envelope.work_item_id)
                || hint.id != origin_id(&envelope.work_item_id)
            {
                return Err(changed());
            }
            let (expected_work, expected_run) =
                original_parent(&envelope.origin)?.map_or((None, None), |(work, run)| {
                    (
                        Some(crate::surreal::thing_from_record_id(work)),
                        Some(crate::surreal::thing_from_record_id(run)),
                    )
                });
            if hint.parent_work_item_id != expected_work || hint.parent_work_run_id != expected_run
            {
                return Err(changed());
            }
            if origins
                .insert(envelope.work_item_id.clone(), envelope)
                .is_some()
            {
                return Err(changed());
            }
        }

        // Query hints have no authority. The complete ordered original payload
        // chain, including its genuine terminal, must match every hinted ID.
        let mut expected = Some(parent.base.id.clone());
        let mut work_ids = Vec::with_capacity(chain.work_ids.len());
        let mut work_set = BTreeSet::new();
        let mut run_ids = BTreeSet::new();
        for hinted in chain.work_ids {
            let id = expected.take().ok_or_else(changed)?;
            if hinted != crate::surreal::thing_from_record_id(&id) || !work_set.insert(id.clone()) {
                return Err(changed());
            }
            let envelope = origins.get(&id).ok_or_else(changed)?;
            expected = match original_parent(&envelope.origin)? {
                Some((prior, run)) => {
                    run_ids.insert(run.clone());
                    Some(prior.clone())
                }
                None => None,
            };
            work_ids.push(id);
        }
        if expected.is_some() || origins.len() != work_ids.len() {
            return Err(changed());
        }

        // Read only actual original targets. Eligible hot rows have retention
        // precedence; an archive is needed only for an ID absent from this read.
        // These are observations, never cached authority: child admission still
        // rechecks every original row and private control in its transaction.
        let query = format!(
            "SELECT {WORK_ITEM_PROJECTION} FROM $observed_works WHERE space_id=$space AND deleted_at IS NONE;\n\
             SELECT {WORK_RUN_PROJECTION} FROM $observed_runs WHERE space_id=$space AND deleted_at IS NONE;"
        );
        let mut response = repository
            .client
            .query_bind_checked(
                &query,
                crate::bindings! {
                    "space" => space.clone(),
                    "observed_works" => record_targets(work_ids.iter().cloned()),
                    "observed_runs" => record_targets(run_ids.iter().cloned()),
                },
            )
            .await?;
        let hot_works: Vec<WorkItemDb> = response.take(0).map_err(invalid)?;
        let hot_runs: Vec<WorkRunDb> = response.take(1).map_err(invalid)?;
        let mut works = BTreeMap::new();
        let mut runs = BTreeMap::new();
        for row in hot_works {
            let work = WorkItem::try_from(row)?;
            if !work_set.contains(&work.base.id) || work.base.space_id != *space {
                return Err(changed());
            }
            if works.insert(work.base.id.clone(), work).is_some() {
                return Err(changed());
            }
        }
        for row in hot_runs {
            let run = WorkRun::try_from(row)?;
            if !run_ids.contains(&run.base.id) || run.base.space_id != *space {
                return Err(changed());
            }
            if runs.insert(run.base.id.clone(), run).is_some() {
                return Err(changed());
            }
        }
        let missing_works = work_set
            .iter()
            .filter(|id| !works.contains_key(*id))
            .cloned()
            .collect::<BTreeSet<_>>();
        let missing_runs = run_ids
            .iter()
            .filter(|id| !runs.contains_key(*id))
            .cloned()
            .collect::<BTreeSet<_>>();
        if !missing_works.is_empty() || !missing_runs.is_empty() {
            let query = format!(
                "SELECT {WORK_ITEM_PROJECTION} FROM (SELECT VALUE record FROM $observed_work_archives WHERE space_id=$space AND deleted_at IS NONE);\n\
                 SELECT {WORK_RUN_PROJECTION} FROM (SELECT VALUE record FROM $observed_run_archives WHERE space_id=$space AND deleted_at IS NONE);"
            );
            let mut response = repository
                .client
                .query_bind_checked(
                    &query,
                    crate::bindings! {
                        "space" => space.clone(),
                        "observed_work_archives" => record_targets(missing_works.iter().map(crate::work_retention::archive_id)),
                        "observed_run_archives" => record_targets(missing_runs.iter().map(crate::work_retention::archive_id)),
                    },
                )
                .await?;
            let cold_works: Vec<WorkItemDb> = response.take(0).map_err(invalid)?;
            let cold_runs: Vec<WorkRunDb> = response.take(1).map_err(invalid)?;
            for row in cold_works {
                let work = WorkItem::try_from(row)?;
                if !missing_works.contains(&work.base.id) || work.base.space_id != *space {
                    return Err(changed());
                }
                if works.insert(work.base.id.clone(), work).is_some() {
                    return Err(changed());
                }
            }
            for row in cold_runs {
                let run = WorkRun::try_from(row)?;
                if !missing_runs.contains(&run.base.id) || run.base.space_id != *space {
                    return Err(changed());
                }
                if runs.insert(run.base.id.clone(), run).is_some() {
                    return Err(changed());
                }
            }
        }
        if works.len() != work_ids.len() || runs.len() != run_ids.len() {
            return Err(changed());
        }
        Ok(Self {
            works,
            runs,
            origins,
        })
    }

    pub(super) fn origin(&self, work: &WorkItem) -> StorageResult<OriginEnvelope> {
        let envelope = self.origins.get(&work.base.id).ok_or_else(changed)?;
        envelope.verify(work)?;
        Ok(envelope.clone())
    }

    pub(super) fn parent(
        &self,
        space: &str,
        work_id: &RecordId,
        run_id: &RecordId,
    ) -> StorageResult<(WorkItem, WorkRun, OriginEnvelope)> {
        let work = self.works.get(work_id).ok_or_else(changed)?;
        let run = self.runs.get(run_id).ok_or_else(changed)?;
        if work.base.space_id != space || run.base.space_id != space {
            return Err(changed());
        }
        Ok((work.clone(), run.clone(), self.origin(work)?))
    }
}
