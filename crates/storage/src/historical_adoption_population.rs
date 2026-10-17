//! One complete lineage scan under transfer/catch-up custody. Group commits
//! retain exact full row guards; this index proves no active revision/node was omitted.
use super::*;
const MAX_ROWS: usize = 2_000_000;
#[derive(Debug, Default)]
pub(super) struct HistoricalPopulation {
    revisions: BTreeMap<RecordId, BTreeSet<RecordId>>,
    nodes: BTreeMap<RecordId, BTreeSet<RecordId>>,
    rows: usize,
    epoch: std::sync::Mutex<Option<String>>,
}
impl HistoricalPopulation {
    fn insert(
        &mut self,
        table: TableName,
        id: RecordId,
        parent: RecordId,
        deleted: bool,
        active: bool,
    ) -> StorageResult<()> {
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| integrity("historical population overflow"))?;
        let expected_parent = match table {
            TableName::DocRevision => TableName::Doc,
            TableName::DocNode => TableName::DocRevision,
            _ => return Err(integrity("historical population table")),
        };
        if self.rows > MAX_ROWS || id.table() != table || parent.table() != expected_parent {
            return Err(integrity("historical population bound/lineage differs"));
        }
        if deleted || !active {
            return Ok(());
        }
        let map = if table == TableName::DocRevision {
            &mut self.revisions
        } else {
            &mut self.nodes
        };
        if !map.entry(parent).or_default().insert(id) {
            return Err(integrity("historical population duplicate"));
        }
        Ok(())
    }
    pub(super) fn verify_epoch(&self, epoch: Option<&str>) -> StorageResult<()> {
        if self
            .epoch
            .lock()
            .map_err(|_| integrity("historical population lock"))?
            .as_deref()
            != epoch
        {
            return Err(integrity("historical indexed population epoch changed"));
        }
        Ok(())
    }
    pub(super) fn advance_epoch(&self, before: Option<&str>, after: &str) -> StorageResult<()> {
        let mut epoch = self
            .epoch
            .lock()
            .map_err(|_| integrity("historical population lock"))?;
        if epoch.as_deref() != before {
            return Err(integrity("historical population commit epoch changed"));
        }
        *epoch = Some(after.into());
        drop(epoch);
        Ok(())
    }
    pub(super) fn verify(&self, row: &HistoricalOriginalDocument) -> StorageResult<()> {
        self.verify_lineage(
            &row.document.base.id,
            &row.revision.base.id,
            row.nodes.iter().map(|node| &node.base.id),
        )
    }
    fn verify_lineage<'a>(
        &self,
        doc: &RecordId,
        revision: &RecordId,
        nodes: impl Iterator<Item = &'a RecordId>,
    ) -> StorageResult<()> {
        let expected = BTreeSet::from([revision.clone()]);
        if self.revisions.get(doc) != Some(&expected) {
            return Err(integrity("historical active revision population differs"));
        }
        let mut declared = BTreeSet::new();
        for id in nodes {
            if !declared.insert(id.clone()) {
                return Err(integrity("historical duplicate node population"));
            }
        }
        if self
            .nodes
            .get(revision)
            .map_or(!declared.is_empty(), |actual| actual != &declared)
        {
            return Err(integrity("historical complete node population differs"));
        }
        Ok(())
    }
}
#[derive(Debug, Deserialize, PartialEq)]
struct NativeLineage {
    id: surrealdb::sql::Thing,
    space_id: String,
    parent: surrealdb::sql::Thing,
    deleted: bool,
    revision_status: Option<RevisionStatus>,
}
// The typed population accepts only UUID and UUID-string identities. Native
// physical order equals semantic order for those scalar keys; malformed other
// identities still fail the same full lineage conversion below. A native range
// seeks after the exact physical cursor rather than re-sorting the whole table.
fn lineage_range(
    table: TableName,
    cursor: Option<&surrealdb::sql::Thing>,
) -> StorageResult<surrealdb::sql::Thing> {
    use std::ops::Bound;
    use surrealdb::sql::{Id, IdRange, Thing};
    if !matches!(table, TableName::DocRevision | TableName::DocNode) {
        return Err(integrity("historical lineage range table invalid"));
    }
    let lower = if let Some(previous) = cursor {
        if previous.tb != table.as_str() {
            return Err(integrity("historical lineage cursor table differs"));
        }
        crate::surreal::record_id_from_thing(previous)
            .map_err(|_| integrity("historical lineage cursor identity invalid"))?;
        Bound::Excluded(previous.id.clone())
    } else {
        Bound::Unbounded
    };
    Ok(Thing::from((
        table.as_str(),
        Id::Range(Box::new(IdRange {
            beg: lower,
            end: Bound::Unbounded,
        })),
    )))
}
async fn native_lineage_page(
    client: &StorageClient,
    table: TableName,
    cursor: Option<&surrealdb::sql::Thing>,
) -> StorageResult<Vec<NativeLineage>> {
    let range = lineage_range(table, cursor)?;
    let (parent, status) = if table == TableName::DocRevision {
        ("doc", "status")
    } else {
        ("revision", "NULL")
    };
    let sql = format!("SELECT id,space_id,{parent} AS parent,deleted_at IS NOT NONE AS deleted,{status} AS revision_status FROM $range LIMIT 256");
    let mut response = client
        .query_bind_checked(&sql, crate::bindings! { "range" => range })
        .await?;
    response
        .take(0)
        .map_err(|_| integrity("historical lineage page invalid"))
}
impl PreparedHistoricalAuthority {
    pub(super) async fn prepare_population(&self) -> StorageResult<HistoricalPopulation> {
        let started = std::time::Instant::now();
        self.check_custody()?;
        let epoch = CanonicalRepository::new(self.client.clone())
            .observe_epoch(&self.input.space_id)
            .await?;
        let mut population = HistoricalPopulation::default();
        let mut physical_scanned = 0_usize;
        for table in [TableName::DocRevision, TableName::DocNode] {
            if let Some(store) = self.client.embedded_store() {
                for (id, parent, deleted, active) in store
                    .historical_population(table, &self.input.space_id, MAX_ROWS)
                    .await?
                {
                    population.insert(table, id, parent, deleted, active)?;
                }
            } else {
                let mut cursor: Option<surrealdb::sql::Thing> = None;
                loop {
                    let rows = native_lineage_page(&self.client, table, cursor.as_ref()).await?;
                    if rows.len() > 256 {
                        return Err(integrity("historical lineage page bound"));
                    }
                    if rows.is_empty() {
                        break;
                    }
                    for row in rows {
                        if cursor.as_ref().is_some_and(|before| &row.id <= before) {
                            return Err(integrity("historical lineage cursor differs"));
                        }
                        physical_scanned = physical_scanned
                            .checked_add(1)
                            .ok_or_else(|| integrity("historical physical population overflow"))?;
                        if physical_scanned > MAX_ROWS {
                            return Err(integrity("historical physical population bound"));
                        }
                        let id = crate::surreal::record_id_from_thing(&row.id)
                            .map_err(|_| integrity("historical lineage id invalid"))?;
                        let parent = crate::surreal::record_id_from_thing(&row.parent)
                            .map_err(|_| integrity("historical lineage parent invalid"))?;
                        let parent_table = if table == TableName::DocRevision {
                            TableName::Doc
                        } else {
                            TableName::DocRevision
                        };
                        if id.table() != table
                            || parent.table() != parent_table
                            || row.space_id.is_empty()
                        {
                            return Err(integrity("historical physical lineage invalid"));
                        }
                        let active = if table == TableName::DocRevision {
                            row.revision_status
                                .ok_or_else(|| integrity("historical revision status absent"))?
                                == RevisionStatus::Active
                        } else {
                            true
                        };
                        cursor = Some(row.id.clone());
                        if row.space_id == self.input.space_id {
                            population.insert(table, id, parent, row.deleted, active)?;
                        }
                    }
                }
            }
        }
        self.check_custody()?;
        if CanonicalRepository::new(self.client.clone())
            .observe_epoch(&self.input.space_id)
            .await?
            != epoch
        {
            return Err(integrity("historical population epoch changed"));
        }
        population.epoch = std::sync::Mutex::new(epoch);
        tracing::debug!(
            target: "recall_transfer_timing",
            kind = "historical_population",
            physical_rows = physical_scanned,
            retained_space_rows = population.rows,
            seconds = started.elapsed().as_secs_f64(),
            "closed historical lineage scan completed"
        );
        Ok(population)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    #[test]
    fn lineage_population_refuses_omission_extra_active_revision_and_duplicate() {
        let doc = RecordId::new_v4(TableName::Doc);
        let revision = RecordId::new_v4(TableName::DocRevision);
        let node = RecordId::new_v4(TableName::DocNode);
        let mut index = HistoricalPopulation::default();
        index
            .insert(
                TableName::DocRevision,
                revision.clone(),
                doc.clone(),
                false,
                true,
            )
            .unwrap();
        index
            .insert(
                TableName::DocNode,
                node.clone(),
                revision.clone(),
                false,
                true,
            )
            .unwrap();
        index
            .verify_lineage(&doc, &revision, std::iter::once(&node))
            .unwrap();
        assert!(index
            .verify_lineage(&doc, &revision, std::iter::empty())
            .is_err());
        assert!(index
            .verify_lineage(&doc, &revision, [&node, &node].into_iter())
            .is_err());
        index
            .insert(
                TableName::DocRevision,
                RecordId::new_v4(TableName::DocRevision),
                doc.clone(),
                false,
                false,
            )
            .unwrap();
        index
            .verify_lineage(&doc, &revision, std::iter::once(&node))
            .unwrap();
        index
            .insert(
                TableName::DocRevision,
                RecordId::new_v4(TableName::DocRevision),
                doc.clone(),
                false,
                true,
            )
            .unwrap();
        assert!(index
            .verify_lineage(&doc, &revision, std::iter::once(&node))
            .is_err());
    }
    #[test]
    fn population_epoch_changes_only_from_exact_guarded_predecessor() {
        let index = HistoricalPopulation::default();
        index.verify_epoch(None).unwrap();
        index.advance_epoch(None, "first").unwrap();
        assert!(index.verify_epoch(None).is_err());
        assert!(index.advance_epoch(None, "unrelated").is_err());
        index.verify_epoch(Some("first")).unwrap();
        index.advance_epoch(Some("first"), "second").unwrap();
        index.verify_epoch(Some("second")).unwrap();
    }
    #[test]
    fn population_rejects_invalid_lineage_and_bound_even_for_deleted_rows() {
        let mut index = HistoricalPopulation::default();
        assert!(index
            .insert(
                TableName::DocNode,
                RecordId::new_v4(TableName::DocNode),
                RecordId::new_v4(TableName::Doc),
                true,
                true
            )
            .is_err());
        index.rows = MAX_ROWS;
        assert!(index
            .insert(
                TableName::DocNode,
                RecordId::new_v4(TableName::DocNode),
                RecordId::new_v4(TableName::DocRevision),
                true,
                true
            )
            .is_err());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // One ordered native range/population drift scenario.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original transfer client, population authority and epoch reader through all range, drift, malformed-lineage and recovery assertions."
    )]
    async fn native_population_range_matches_ordered_pages_and_rejects_drift() {
        use crate::historical_adoption::tests::{declaration, document, plan};
        use surrealdb::sql::{Id, Thing, Value};
        let client = StorageClient::new_memory().await.unwrap();
        let input = plan(vec![declaration(&document())]);
        let snapshot = input.snapshot_sha256.clone();
        client.with_transfer_barrier(&snapshot, move |exclusive| async move {
            // This fixture intentionally isolates physical population projection:
            // all rows are native; malformed lineage remains insertable so the
            // production population validator, rather than schema, must reject it.
            exclusive.query_checked(include_str!("../../migrations/sql/034_storage_durability.surql")).await?;
            exclusive.query_checked("DEFINE FIELD IF NOT EXISTS reference_key_fingerprint ON TABLE recall_durability TYPE string;").await?;
            exclusive.initialize_journal_identity().await?;
            exclusive.query_checked("DEFINE TABLE canonical_epoch SCHEMALESS; DEFINE TABLE doc_revision SCHEMALESS; DEFINE TABLE doc_node SCHEMALESS; CREATE canonical_epoch:⟨history-test⟩ SET space_id='history-test',epoch='population-before';").await?;
            let mut expected = HistoricalPopulation::default();
            let mut native_revisions = Vec::new();
            let mut native_nodes = Vec::new();
            for i in 0_u128..521 {
                let revision_uuid = uuid::Uuid::from_u128(1000 + i);
                let node_uuid = uuid::Uuid::from_u128(2000 + i);
                let doc_uuid = uuid::Uuid::from_u128(3000 + i);
                let revision_key = if i % 2 == 0 { Id::Uuid(revision_uuid.into()) } else { Id::String(revision_uuid.to_string()) };
                let node_key = if i % 2 == 0 { Id::String(node_uuid.to_string()) } else { Id::Uuid(node_uuid.into()) };
                let revision = Thing::from(("doc_revision", revision_key));
                let node = Thing::from(("doc_node", node_key));
                let doc = Thing::from(("doc", Id::Uuid(doc_uuid.into())));
                let space = if i % 17 == 0 { "foreign" } else { "history-test" };
                let deleted = i % 11 == 0;
                let active = i % 13 != 0;
                let deleted_at = if deleted { crate::surreal::datetime_from_chrono(Utc::now()).into() } else { Value::None };
                let revision_value = Value::Object(surrealdb::sql::Object::from(BTreeMap::<String, Value>::from([
                    ("id".into(), revision.clone().into()), ("space_id".into(), space.into()),
                    ("doc".into(), doc.clone().into()), ("deleted_at".into(), deleted_at.clone()),
                    ("status".into(), if active { "active" } else { "superseded" }.into()),
                ])));
                let node_value = Value::Object(surrealdb::sql::Object::from(BTreeMap::<String, Value>::from([
                    ("id".into(), node.clone().into()), ("space_id".into(), space.into()),
                    ("revision".into(), revision.clone().into()), ("deleted_at".into(), deleted_at),
                ])));
                native_revisions.push((revision.clone(), revision_value));
                native_nodes.push((node.clone(), node_value));
                if space == "history-test" {
                    expected.insert(TableName::DocRevision, RecordId::new(TableName::DocRevision,revision_uuid),RecordId::new(TableName::Doc,doc_uuid),deleted,active)?;
                    expected.insert(TableName::DocNode,RecordId::new(TableName::DocNode,node_uuid),RecordId::new(TableName::DocRevision,revision_uuid),deleted,true)?;
                }
            }
            for records in [&native_revisions, &native_nodes] {
                for chunk in records.chunks(128) {
                    let mut sql=String::from("BEGIN TRANSACTION;");
                    let mut bindings=crate::bindings! {};
                    for (i,(id,value)) in chunk.iter().enumerate() {
                        bindings.insert(format!("id{i}"),Value::Thing(id.clone()));
                        bindings.insert(format!("value{i}"),value.clone());
                        write!(sql, "CREATE $id{i} CONTENT $value{i} RETURN NONE;").map_err(|_| integrity("native population fixture SQL formatting"))?;
                    }
                    sql.push_str("COMMIT TRANSACTION;");
                    exclusive.query_bind_checked(&sql,bindings).await?;
                }
            }
            for table in [TableName::DocRevision,TableName::DocNode] {
                let (parent,status)=if table==TableName::DocRevision {("doc","status")} else {("revision","NULL")};
                let mut ordered=exclusive.query_checked(&format!("SELECT id,space_id,{parent} AS parent,deleted_at IS NOT NONE AS deleted,{status} AS revision_status FROM {table} ORDER BY id")).await?;
                let ordered:Vec<NativeLineage>=ordered.take(0).unwrap();
                let mut actual=Vec::new();
                let mut sizes=Vec::new();
                let mut cursor=None;
                loop {
                    let page=native_lineage_page(&exclusive,table,cursor.as_ref()).await?;
                    sizes.push(page.len());
                    if page.is_empty() { break; }
                    cursor=page.last().map(|row|row.id.clone());
                    actual.extend(page);
                }
                assert_eq!(sizes,vec![256,256,9,0]);
                assert_eq!(actual,ordered,"full native identity/lineage order for {table}");
                let mut explain=exclusive.query_bind_checked("SELECT id FROM $range LIMIT 256 EXPLAIN",crate::bindings! {"range"=>lineage_range(table,None)?}).await?;
                let explain:surrealdb::Value=explain.take(0).unwrap();
                assert!(explain.to_string().contains("Iterate Range"),"{explain}");
                assert!(!explain.to_string().contains("Iterate Table"),"{explain}");
            }
            let authority=HistoricalAdoptionRepository::new(exclusive.clone()).prepare_authority(input)?;
            let population=authority.prepare_population().await?;
            assert_eq!(population.revisions,expected.revisions);
            assert_eq!(population.nodes,expected.nodes);
            assert_eq!(population.rows,expected.rows);
            let epochs=CanonicalRepository::new(exclusive.clone());
            let before=epochs.observe_epoch("history-test").await?;
            population.verify_epoch(before.as_deref())?;
            exclusive.query_checked("UPDATE canonical_epoch:⟨history-test⟩ SET epoch='population-after';").await?;
            let after=epochs.observe_epoch("history-test").await?;
            assert_ne!(before,after);
            assert!(population.verify_epoch(after.as_deref()).is_err());
            assert!(population.advance_epoch(after.as_deref(),"not-authorized").is_err());
            population.verify_epoch(before.as_deref())?;
            // The closed scan must expose omitted nodes and a second active
            // revision, not merely preserve pagination of the happy path.
            let target_doc=RecordId::new(TableName::Doc,uuid::Uuid::from_u128(3001));
            let target_revision=RecordId::new(TableName::DocRevision,uuid::Uuid::from_u128(1001));
            let target_node=RecordId::new(TableName::DocNode,uuid::Uuid::from_u128(2001));
            population.verify_lineage(&target_doc,&target_revision,std::iter::once(&target_node))?;
            exclusive.query_bind_checked("DELETE $id;",crate::bindings! {"id"=>native_nodes[1].0.clone()}).await?;
            assert!(authority.prepare_population().await?.verify_lineage(&target_doc,&target_revision,std::iter::once(&target_node)).is_err());
            exclusive.query_bind_checked("CREATE $id CONTENT $row;",crate::bindings! {"id"=>native_nodes[1].0.clone(),"row"=>native_nodes[1].1.clone()}).await?;
            let extra=Thing::from(("doc_revision",Id::Uuid(uuid::Uuid::new_v4().into())));
            exclusive.query_bind_checked("CREATE $id SET space_id='history-test',doc=$doc,status='active',deleted_at=NONE;",crate::bindings! {"id"=>extra.clone(),"doc"=>crate::surreal::thing_from_record_id(&target_doc)}).await?;
            assert!(authority.prepare_population().await?.verify_lineage(&target_doc,&target_revision,std::iter::once(&target_node)).is_err());
            exclusive.query_bind_checked("DELETE $id;",crate::bindings! {"id"=>extra}).await?;
            // A malformed foreign row cannot disappear behind namespace filtering.
            let malformed_id=Thing::from(("doc_node",Id::Uuid(uuid::Uuid::new_v4().into())));
            exclusive.query_bind_checked("CREATE $id SET space_id='foreign',revision=$wrong,deleted_at=NONE;",crate::bindings! {"id"=>malformed_id.clone(),"wrong"=>Thing::from(("doc",Id::Uuid(uuid::Uuid::new_v4().into())))}).await?;
            assert!(authority.prepare_population().await.unwrap_err().to_string().contains("historical physical lineage invalid"));
            exclusive.query_bind_checked("DELETE $id;",crate::bindings! {"id"=>malformed_id}).await?;
            let malformed_id=Thing::from(("doc_node",Id::Array(surrealdb::sql::Array::from(vec![Value::from(1i64)]))));
            exclusive.query_bind_checked("CREATE $id SET space_id='foreign',revision=$revision,deleted_at=NONE;",crate::bindings! {"id"=>malformed_id.clone(),"revision"=>native_revisions[0].0.clone()}).await?;
            assert!(authority.prepare_population().await.unwrap_err().to_string().contains("historical lineage id invalid"));
            assert!(lineage_range(TableName::DocNode,Some(&malformed_id)).is_err());
            assert!(lineage_range(TableName::DocNode,Some(&native_revisions[0].0)).is_err());
            exclusive.query_bind_checked("DELETE $id;",crate::bindings! {"id"=>malformed_id}).await?;
            authority.prepare_population().await?;
            Ok(())
        }).await.unwrap();
    }
}
