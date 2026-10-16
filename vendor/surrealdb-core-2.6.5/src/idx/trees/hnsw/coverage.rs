//! Bounded physical HNSW coverage in the caller's transaction.
//! No provider, mutation, graph scan, approximate query, or cached membership.
use super::{ElementId, HnswState};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::idx::trees::vector::Vector;
use crate::idx::{IndexKeyBase, VersionedStore};
use crate::kvs::Transaction;
use crate::sql::index::{Distance, Index, VectorType};
use crate::sql::{Array, Base, Value};
use std::collections::BTreeSet;

fn invalid() -> Error {
    Error::InvalidArguments { name: "recall::hnsw_coverage".into(), message: "Expected bounded distinct native row IDs and finite nonzero vectors for one F32 cosine partition".into() }
}

/// Input row objects carry the exact `id` and `vector` already captured by the
/// repository. That repository also CASes each full native row and authority.
pub(crate) async fn check(
    (ctx, opt): (&Context, &Options),
    (table, index, rows): (String, String, Array),
) -> Result<Value, Error> {
    opt.is_allowed(Action::View, ResourceKind::Index, &Base::Db)?;
    if rows.is_empty() || rows.len() > 100_000 {
        return Err(invalid());
    }
    let (ns, db) = opt.ns_db()?;
    let tx = ctx.tx();
    let definition = tx.get_tb_index(ns, db, &table, &index).await?;
    let Index::Hnsw(params) = &definition.index else {
        return Ok(false.into());
    };
    if params.vector_type != VectorType::F32
        || params.distance != Distance::Cosine
        || definition.cols.len() != 1
        || definition.cols[0].to_string() != "vector"
        || definition.defer
        || definition.what.as_str() != table
        || definition.name.as_str() != index
    {
        return Ok(false.into());
    }
    // Ready is checked once per bounded partition, not once per node.
    let builder = ctx.get_index_builder().ok_or_else(invalid)?;
    let status: Value = builder.get_status(ns, db, &definition).await.into();
    let Value::Object(status) = status else {
        return Ok(false.into());
    };
    if status.get("status") != Some(&Value::from("ready")) {
        return Ok(false.into());
    }
    if let Some(pending) = status.get("pending") {
        if pending != &Value::from(0i64) {
            return Ok(false.into());
        }
    }
    let ikb = IndexKeyBase::new(ns, db, &definition)?;
    let Some(state) = graph_state(&tx, &ikb).await? else {
        return Ok(false.into());
    };
    let dims = usize::from(params.dimension);
    let mut seen = BTreeSet::new();
    let mut components = 0usize;
    for row in rows {
        let Value::Object(row) = row else {
            return Err(invalid());
        };
        if row.len() != 2 {
            return Err(invalid());
        }
        let Some(Value::Thing(id)) = row.get("id") else {
            return Err(invalid());
        };
        let Some(Value::Array(values)) = row.get("vector") else {
            return Err(invalid());
        };
        components = components.checked_add(values.len()).ok_or_else(invalid)?;
        if id.tb != table
            || !seen.insert(id.clone())
            || values.len() != dims
            || components > 16_777_216
        {
            return Err(invalid());
        }
        let mut nonzero = false;
        for value in values.iter() {
            let Value::Number(number) = value else {
                return Err(invalid());
            };
            let component = number.to_float();
            if !component.is_finite() || component.abs() > f64::from(f32::MAX) {
                return Err(invalid());
            }
            nonzero |= (component as f32) != 0.0;
        }
        if !nonzero {
            return Err(invalid());
        }
        let vector = Vector::try_from_value(VectorType::F32, dims, &Value::Array(values.clone()))?;
        let Some(element) = super::docs::exact_member(&tx, &ikb, &id.id, &vector).await? else {
            return Ok(false.into());
        };
        if element >= state.next_element_id
            || !layer_member(&tx, &ikb, 0, element, state.next_element_id).await?
        {
            return Ok(false.into());
        }
    }
    Ok(true.into())
}

// Coverage follows current per-node HN format. A legacy HL-only graph must
// first complete the normal reindex writer's migration; certification never
// migrates it or loads an unbounded graph as a side effect.
async fn graph_state(tx: &Transaction, ikb: &IndexKeyBase) -> Result<Option<HnswState>, Error> {
    let Some(bytes) = tx.get(ikb.new_hs_key()?, None).await? else {
        return Ok(None);
    };
    let state: HnswState = VersionedStore::try_from(bytes)?;
    let Some(entry) = state.enter_point else {
        return Ok(None);
    };
    if state.layer0.chunks != 0
        || state.layer0.version == 0
        || state.layers.len() > 64
        || entry >= state.next_element_id
    {
        return Ok(None);
    }
    let Some(bytes) = tx.get(ikb.new_he_key(entry)?, None).await? else {
        return Ok(None);
    };
    let _: crate::idx::trees::vector::SerializedVector = VersionedStore::try_from(bytes)?;
    if !layer_member(tx, ikb, 0, entry, state.next_element_id).await? {
        return Ok(None);
    }
    for (level, layer) in state.layers.iter().enumerate() {
        if layer.chunks != 0
            || layer.version == 0
            || !layer_member(tx, ikb, (level + 1) as u16, entry, state.next_element_id).await?
        {
            return Ok(None);
        }
    }
    Ok(Some(state))
}
async fn layer_member(
    tx: &Transaction,
    ikb: &IndexKeyBase,
    level: u16,
    element: ElementId,
    next: ElementId,
) -> Result<bool, Error> {
    let Some(bytes) = tx.get(ikb.new_hn_key(level, element)?, None).await? else {
        return Ok(false);
    };
    let Some(prefix) = bytes.get(..2) else {
        return Ok(false);
    };
    let count = usize::from(u16::from_be_bytes([prefix[0], prefix[1]]));
    if bytes.len() != 2 + count * 8 {
        return Ok(false);
    }
    let mut seen = BTreeSet::new();
    for edge in bytes[2..].chunks_exact(8) {
        let edge = u64::from_be_bytes(edge.try_into().map_err(|_| invalid())?);
        if edge >= next || edge == element || !seen.insert(edge) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbs::Session;
    use crate::kvs::{Datastore, LockType::Optimistic, TransactionType::Write};
    use crate::sql::{Id, Object, Thing};
    use std::{collections::BTreeMap, sync::Arc};

    fn rows(count: usize, vector: Vec<Value>) -> Array {
        (0..count)
            .map(|n| {
                Value::Object(Object(BTreeMap::from([
                    (
                        "id".into(),
                        Thing::from(("points", Id::Number(n as i64))).into(),
                    ),
                    ("vector".into(), Value::Array(Array(vector.clone()))),
                ])))
            })
            .collect::<Vec<_>>()
            .into()
    }

    // Twelve identical vectors exercise the persisted bitmap representation,
    // not just the small set or nearest-neighbor search's result selection.
    #[tokio::test]
    async fn physical_coverage_requires_every_identity_bitmap_element_and_graph_key() {
        let ds = Datastore::new("memory").await.unwrap();
        let session = Session::owner().with_ns("coverage").with_db("test");
        let mut sql =
            "DEFINE INDEX knn ON points FIELDS vector HNSW DIMENSION 2 TYPE F32 DIST COSINE;"
                .to_owned();
        for n in 0..12 {
            sql.push_str(&format!("CREATE points:{n} SET vector=[1,0];"));
        }
        for result in ds.execute(&sql, &session, None).await.unwrap() {
            result.result.unwrap();
        }
        let tx = Arc::new(ds.transaction(Write, Optimistic).await.unwrap());
        let mut ctx = ds.setup_ctx().unwrap();
        ctx.set_transaction(tx.clone());
        let ctx = ctx.freeze();
        let opt = ds.setup_options(&session);
        let input = rows(12, vec![1.into(), 0.into()]);
        assert_eq!(
            check((&ctx, &opt), ("points".into(), "knn".into(), input.clone()))
                .await
                .unwrap(),
            Value::Bool(true)
        );
        let definition = tx
            .get_tb_index("coverage", "test", "points", "knn")
            .await
            .unwrap();
        let ikb = IndexKeyBase::new("coverage", "test", &definition).unwrap();
        let vector = Vector::try_from_value(
            VectorType::F32,
            2,
            &Value::Array(vec![1.into(), 0.into()].into()),
        )
        .unwrap();
        let element = super::super::docs::exact_member(&tx, &ikb, &Id::Number(11), &vector)
            .await
            .unwrap()
            .unwrap();
        let hi = ikb.new_hi_key(Id::Number(11)).unwrap();
        let doc_id = u64::from_be_bytes(
            tx.get(hi.clone(), None)
                .await
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        let state = graph_state(&tx, &ikb).await.unwrap().unwrap();
        let entry = state.enter_point.unwrap();
        let mut keys = BTreeSet::from([
            hi,
            ikb.new_hd_key(Some(doc_id)).unwrap(),
            ikb.new_hv_key(Arc::new((&vector).into())).unwrap(),
            ikb.new_he_key(element).unwrap(),
            ikb.new_hs_key().unwrap(),
            ikb.new_hn_key(0, element).unwrap(),
            ikb.new_hn_key(0, entry).unwrap(),
        ]);
        for level in 1..=state.layers.len() {
            keys.insert(ikb.new_hn_key(level as u16, entry).unwrap());
        }
        for key in keys {
            let original = tx.get(key.clone(), None).await.unwrap().unwrap();
            tx.del(key.clone()).await.unwrap();
            assert_eq!(
                check((&ctx, &opt), ("points".into(), "knn".into(), input.clone()))
                    .await
                    .unwrap(),
                Value::Bool(false)
            );
            tx.set(key, original, None).await.unwrap();
        }
        // Full identity roundtrip, not existence of some reverse mapping.
        let hd = ikb.new_hd_key(Some(doc_id)).unwrap();
        let original = tx.get(hd.clone(), None).await.unwrap().unwrap();
        tx.set(hd.clone(), revision::to_vec(&Id::Number(10)).unwrap(), None)
            .await
            .unwrap();
        assert_eq!(
            check((&ctx, &opt), ("points".into(), "knn".into(), input.clone()))
                .await
                .unwrap(),
            Value::Bool(false)
        );
        tx.set(hd, original, None).await.unwrap();
        assert_eq!(
            check(
                (&ctx, &opt),
                (
                    "points".into(),
                    "knn".into(),
                    rows(12, vec![0.into(), 1.into()])
                )
            )
            .await
            .unwrap(),
            Value::Bool(false)
        );
        assert!(check(
            (&ctx, &opt),
            ("points".into(), "absent".into(), input.clone())
        )
        .await
        .is_err());
        let mut duplicate = input.clone();
        duplicate.push(input[0].clone());
        assert!(
            check((&ctx, &opt), ("points".into(), "knn".into(), duplicate))
                .await
                .is_err()
        );
        assert!(check(
            (&ctx, &opt),
            (
                "points".into(),
                "knn".into(),
                rows(1, vec![Value::from(1e-100f64), 0.into()])
            )
        )
        .await
        .is_err());
        assert_eq!(
            check((&ctx, &opt), ("points".into(), "knn".into(), input))
                .await
                .unwrap(),
            Value::Bool(true)
        );
        tx.cancel().await.unwrap();
        // A normally removed twin cannot be certified from surviving duplicates.
        for result in ds
            .execute("DELETE points:11;", &session, None)
            .await
            .unwrap()
        {
            result.result.unwrap();
        }
        let tx = Arc::new(ds.transaction(Write, Optimistic).await.unwrap());
        let mut ctx = ds.setup_ctx().unwrap();
        ctx.set_transaction(tx.clone());
        let ctx = ctx.freeze();
        assert_eq!(
            check(
                (&ctx, &opt),
                (
                    "points".into(),
                    "knn".into(),
                    rows(12, vec![1.into(), 0.into()])
                )
            )
            .await
            .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            check(
                (&ctx, &opt),
                (
                    "points".into(),
                    "knn".into(),
                    rows(11, vec![1.into(), 0.into()])
                )
            )
            .await
            .unwrap(),
            Value::Bool(true)
        );
        tx.cancel().await.unwrap();
    }
}
