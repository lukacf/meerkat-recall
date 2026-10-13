//! Pure native scalar-key evidence for historical erasure selection.
//!
//! A deferred index's commit clock is not its document's clock. The selector
//! supplies independently retained record versions and compatible inspected
//! schema. This module projects exact native keys; it does not decide ownership.

use crate::ctx::MutableContext;
use crate::dbs::{Capabilities, Options, Session};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::index::scalar_index_keys;
use crate::kvs::recall_history::{MAX_HISTORY_BYTES, MAX_HISTORY_RECORDS};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Id, Index, Part, Thing, Value};
use reblessive::TreeStack;
use revision::Revisioned;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

fn invalid(message: &str) -> Error {
    Error::Ds(format!("Recall historical scalar projection: {message}"))
}

struct Budget {
    deadline: Option<Instant>,
    visited: usize,
}

impl Budget {
    fn check(&mut self, depth: usize) -> Result<(), Error> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(invalid("original checkpoint deadline expired"));
        }
        self.visited = self
            .visited
            .checked_add(1)
            .ok_or_else(|| invalid("input overflow"))?;
        if self.visited > MAX_HISTORY_RECORDS
            || depth
                > usize::try_from(*crate::cnf::MAX_COMPUTATION_DEPTH)
                    .map_err(|_| invalid("depth bound"))?
        {
            return Err(invalid("native computation or input bound exceeded"));
        }
        Ok(())
    }
}

// Deliberately narrower than Value::is_static(), which also accepts some
// functions and expressions. Proof must never consult mutable external state.
fn literal(value: &Value, depth: usize, budget: &mut Budget) -> Result<(), Error> {
    budget.check(depth)?;
    match value {
        Value::None
        | Value::Null
        | Value::Bool(_)
        | Value::Bytes(_)
        | Value::Uuid(_)
        | Value::Number(_)
        | Value::Strand(_)
        | Value::Duration(_)
        | Value::Datetime(_)
        | Value::Geometry(_)
        | Value::Regex(_) => Ok(()),
        Value::Array(values) => {
            for value in values.iter() {
                literal(value, depth + 1, budget)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for value in values.values() {
                literal(value, depth + 1, budget)?;
            }
            Ok(())
        }
        Value::Thing(thing) => match &thing.id {
            Id::Number(_) | Id::String(_) | Id::Uuid(_) => Ok(()),
            Id::Array(values) => {
                for value in values.iter() {
                    literal(value, depth + 1, budget)?;
                }
                Ok(())
            }
            Id::Object(values) => {
                for value in values.values() {
                    literal(value, depth + 1, budget)?;
                }
                Ok(())
            }
            _ => Err(invalid(
                "dynamic record identity has no immutable scalar proof",
            )),
        },
        _ => Err(invalid("dynamic value has no immutable scalar proof")),
    }
}

// Preserve the local result shape while proving that native Value::get cannot
// dereference a record or evaluate a dynamic value. In particular, implicit
// array mapping consumes only Field (or Field + All), THEN applies the suffix
// to the mapped array. Applying the entire path independently to each original
// element would prove a different, potentially safe-looking traversal.
// The actual projected keys still use native Document/Indexable below.
fn local_path(
    value: &Value,
    path: &[Part],
    depth: usize,
    budget: &mut Budget,
) -> Result<Value, Error> {
    budget.check(depth)?;
    let Some((part, rest)) = path.split_first() else {
        // A terminal raw value is not evaluated by Value::get. Check literals
        // after the complete path: a later mapped-array index can discard a
        // dynamic sibling without accessing or evaluating it.
        return Ok(value.clone());
    };
    match value {
        Value::Thing(_) => Err(invalid("record dereference has no immutable scalar proof")),
        Value::Object(object) => match part {
            Part::Field(field) => {
                let Some(value) = object.get(field.as_str()) else {
                    return Ok(Value::None);
                };
                // Value::get treats complex document IDs as local objects.
                if field.is_id() && !rest.is_empty() {
                    if let Value::Thing(thing) = value {
                        match &thing.id {
                            Id::Object(id) => {
                                return local_path(
                                    &Value::Object(id.clone()),
                                    rest,
                                    depth + 1,
                                    budget,
                                )
                            }
                            Id::Array(id) => {
                                return local_path(
                                    &Value::Array(id.clone()),
                                    rest,
                                    depth + 1,
                                    budget,
                                )
                            }
                            _ => {}
                        }
                    }
                }
                local_path(value, rest, depth + 1, budget)
            }
            Part::Index(index) => match object.get(&index.to_string()) {
                Some(value) => local_path(value, rest, depth + 1, budget),
                None => Ok(Value::None),
            },
            Part::All => {
                // The native object wildcard produces an array before it
                // evaluates the remainder (which may itself select an index).
                let values = object.values().cloned().collect::<Vec<_>>();
                local_path(&Value::Array(values.into()), rest, depth + 1, budget)
            }
            _ => Ok(Value::None), // Native get returns NONE for flatten on an object.
        },
        Value::Array(values) => match part {
            Part::All | Part::Flatten => values
                .iter()
                .map(|value| local_path(value, rest, depth + 1, budget))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::from),
            Part::Index(index) => match values.get(index.to_usize()) {
                Some(value) => local_path(value, rest, depth + 1, budget),
                None => Ok(Value::None),
            },
            _ => {
                // Native get.rs consumes a following All with the mapped field;
                // other suffix parts operate on the resulting aggregate array.
                let consumed = if matches!(path.get(1), Some(Part::All)) {
                    2
                } else {
                    1
                };
                let mapped = values
                    .iter()
                    .map(|value| local_path(value, &path[..consumed], depth + 1, budget))
                    .collect::<Result<Vec<_>, _>>()?;
                local_path(&Value::from(mapped), &path[consumed..], depth + 1, budget)
            }
        },
        Value::Geometry(geometry) => {
            let value = match part {
                Part::Field(field) if field.is_type() => Value::from(geometry.as_type()),
                Part::Field(field) if field.is_coordinates() && geometry.is_geometry() => {
                    geometry.as_coordinates()
                }
                Part::Field(field) if field.is_geometries() && geometry.is_collection() => {
                    geometry.as_coordinates()
                }
                _ => return Ok(Value::None),
            };
            local_path(&value, rest, depth + 1, budget)
        }
        _ => {
            // Refuse dynamic values even where an ordinary primitive would
            // return NONE. Native Future/Refs have special evaluation branches.
            literal(value, depth, budget)?;
            if matches!(part, Part::Flatten) {
                local_path(value, rest, depth + 1, budget)
            } else {
                Ok(Value::None)
            }
        }
    }
}

struct Size {
    bytes: usize,
    deadline: Option<Instant>,
}
impl std::io::Write for Size {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(std::io::Error::other(
                "original scalar projection deadline expired",
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|size| *size <= MAX_HISTORY_BYTES)
            .ok_or_else(|| std::io::Error::other("native scalar evidence exceeds byte bound"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Exact native scalar keys, with no transaction, I/O, index writes, or workers.
/// Runtime selection must pass its original deadline; `None` is for pure source
/// controls. Dynamic dependencies cannot supply retention evidence.
pub(crate) fn scalar_record_keys(
    namespace: &str,
    database: &str,
    definition: &DefineIndexStatement,
    owner: &Thing,
    document: &Value,
    deadline: Option<Instant>,
) -> Result<BTreeSet<Vec<u8>>, Error> {
    let mut budget = Budget {
        deadline,
        visited: 0,
    };
    budget.check(0)?;
    if namespace.is_empty()
        || database.is_empty()
        || namespace.len() > 4096
        || database.len() > 4096
        || definition.what.as_str() != owner.tb
        || !matches!(definition.index, Index::Idx | Index::Uniq)
        || definition.cols.is_empty()
        || !matches!(document, Value::Object(_))
    {
        return Err(invalid("record, scope or scalar definition differs"));
    }
    if let Value::Object(fields) = document {
        if fields
            .get("id")
            .is_some_and(|id| id != &Value::Thing(owner.clone()))
        {
            return Err(invalid("document identity differs from its native owner"));
        }
    }
    literal(&Value::Thing(owner.clone()), 0, &mut budget)?;
    document
        .serialize_revisioned(&mut Size { bytes: 0, deadline })
        .map_err(|_| invalid("document exceeds native byte bound"))?;
    for column in definition.cols.iter() {
        if !matches!(column.first(), Some(Part::Field(_))) {
            return Err(invalid("index field is not a local native idiom"));
        }
        for (position, part) in column.iter().enumerate() {
            budget.check(position)?;
            if !matches!(part, Part::Field(_) | Part::Index(_) | Part::All)
                && !(matches!(part, Part::Flatten) && position + 1 == column.len())
            {
                return Err(invalid("dynamic index field has no immutable scalar proof"));
            }
        }
        let selected = local_path(document, column, 0, &mut budget)?;
        literal(&selected, 0, &mut budget)?;
    }
    // Every accessed value is a literal and every field access is local. Unrelated
    // dynamic fields are never evaluated. Denying all
    // functions/network is stricter than every original capability policy, so
    // this proof cannot widen that policy or execute a default-enabled function.
    let mut context = MutableContext::background();
    context.add_capabilities(Arc::new(Capabilities::none()));
    if let Some(deadline) = deadline {
        context.add_timeout(
            deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| invalid("original checkpoint deadline expired"))?,
        )?;
    }
    let context = context.freeze();
    let options = Options::new()
        .with_ns(Some(namespace.into()))
        .with_db(Some(database.into()))
        .with_auth(Session::owner().au)
        .with_perms(false);
    let document = CursorDoc::new(Some(Arc::new(owner.clone())), None, document.clone());
    let mut stack = TreeStack::new();
    let values = futures::executor::block_on(
        stack
            .enter(|stk| Document::build_opt_values(stk, &context, &options, definition, &document))
            .finish(),
    )?
    .ok_or_else(|| invalid("retained record has no native index values"))?;
    budget.check(0)?;
    let mut keys = BTreeSet::new();
    let mut bytes = 0usize;
    for key in scalar_index_keys(namespace, database, definition, owner, values) {
        budget.check(0)?;
        let key = key?;
        bytes = bytes
            .checked_add(key.len())
            .filter(|bytes| *bytes <= MAX_HISTORY_BYTES)
            .ok_or_else(|| invalid("projected keys exceed native byte bound"))?;
        keys.insert(key);
    }
    budget.check(0)?;
    Ok(keys)
}

#[cfg(test)]
#[path = "recall_scalar_projection_tests.rs"]
mod tests;
