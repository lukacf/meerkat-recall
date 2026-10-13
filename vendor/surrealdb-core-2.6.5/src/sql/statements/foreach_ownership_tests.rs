//! Direct per-element parity with the frozen b028 unconditional evaluator.
//! These controls bypass range evaluation, not production iteration behavior.
//! Workspace controls separately exercise actual complete native FOR queries.

use super::*;
use crate::dbs::test::mock;
use crate::sql::{Array, Id, Object, Thing};
use crate::syn::Parse;
use std::collections::BTreeMap;

// Exact unconditional expression from b028 foreach.rs:79. Do not route this
// oracle through the production helper or its identity proof.
async fn original(
	v: Value,
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value, Error> {
	stk.run(|stk| v.compute(stk, ctx, opt, doc)).await
}

async fn parity(value: Value, opt: &Options, ctx: &Context) -> Result<Value, Error> {
	let mut stack = reblessive::TreeStack::new();
	let expected = stack
		.enter(|stk| original(value.clone(), stk, ctx, opt, None))
		.finish()
		.await;
	let actual = stack
		.enter(|stk| compute_owned_iteration_value(value, stk, ctx, opt, None))
		.finish()
		.await;
	assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
	actual
}

fn array(values: Vec<Value>) -> Value {
	Value::Array(Array(values))
}

fn object(values: Vec<(&str, Value)>) -> Value {
	Value::Object(Object(values.into_iter().map(|(k, v)| (k.into(), v)).collect()))
}

fn nested(depth: usize, objects: bool) -> Value {
	let mut value = Value::from(7);
	for _ in 0..depth {
		value = if objects {
			object(vec![("child", value)])
		} else {
			array(vec![value])
		};
	}
	value
}

#[tokio::test]
async fn native_foreach_owned_element_retains_original_root_depth_and_caps() {
	let (ctx, base) = mock().await;
	for objects in [false, true] {
		for levels in [0usize, 1, 30, 31, 32, 33, 64] {
			let value = nested(levels, objects);
			// No range evaluation occurs before these direct calls. Each nesting
			// layer plus the scalar needs exactly one original computation dive.
			let required = u32::try_from(levels + 1).unwrap();
			for depth in [0, required.saturating_sub(1), required, required + 1] {
				let opt = base.clone().with_max_computation_depth(depth);
				let result = parity(value.clone(), &opt, &ctx).await;
				assert_eq!(result.is_ok(), depth >= required, "levels={levels}, depth={depth}");
			}
		}
	}
	for value in [array(vec![]), object(vec![])] {
		for depth in [0, 1] {
			let result = parity(value.clone(), &base.clone().with_max_computation_depth(depth), &ctx).await;
			assert_eq!(result.is_ok(), depth == 1);
		}
	}
	for width in [4093usize, 4094, 4095, 4096, 4097, 5000] {
		for value in [
			array((0..width).map(|n| Value::from(n as i64)).collect()),
			Value::Object(Object(
				(0..width).map(|n| (format!("field_{n:04}"), Value::from(n as i64))).collect(),
			)),
		] {
			let result = parity(value.clone(), &base.clone().with_max_computation_depth(2), &ctx).await.unwrap();
			assert_eq!(format!("{result:?}"), format!("{value:?}"));
		}
	}
}

#[tokio::test]
async fn native_foreach_owned_element_retains_native_variants_and_fixed_composite_fallback() {
	let (ctx, opt) = mock().await;
	let fixed = Value::Thing(Thing {
		tb: "original".into(),
		id: Id::Object(Object(BTreeMap::from([("key".into(), Value::from(7))]))),
	});
	let uuid = crate::sql::Uuid::from(
		uuid::Uuid::parse_str("12345678-1234-5678-9234-567812345678").unwrap(),
	);
	let value = object(vec![
		("data", Value::parse("[NONE, NULL, true, 17, 1.25f, 1.250dec, 'text', 7s, d'2025-01-02T03:04:05Z', u'12345678-1234-5678-9234-567812345678', original:7, original:fixed, []]")),
		("uuid_identity", Value::Thing(Thing::from(("original", Id::Uuid(uuid))))),
		("uuid_string_identity", Value::Thing(Thing::from(("original", Id::String(uuid.0.to_string()))))),
		("bytes", Value::Bytes(vec![0_u8, 1, 255].into())),
		("geometry", Value::Geometry((1.25_f64, 2.5_f64).into())),
		("regex", Value::Regex("^(a|b)$".parse().unwrap())),
	]);
	let result = parity(value.clone(), &opt, &ctx).await.unwrap();
	assert_eq!(format!("{result:?}"), format!("{value:?}"));
	let Value::Object(fields) = &result else { panic!("native object result"); };
	assert!(matches!(fields.get("uuid_identity"), Some(Value::Thing(Thing { id: Id::Uuid(id), .. })) if *id == uuid));
	assert!(matches!(fields.get("uuid_string_identity"), Some(Value::Thing(Thing { id: Id::String(id), .. })) if id == &uuid.0.to_string()));
	let Some(Value::Array(data)) = fields.get("data") else { panic!("native array result"); };
	assert!(matches!(data.get(0), Some(Value::None)));
	assert!(matches!(data.get(1), Some(Value::Null)));
	assert!(matches!(data.get(4), Some(Value::Number(crate::sql::Number::Float(_)))));
	assert!(matches!(data.get(5), Some(Value::Number(crate::sql::Number::Decimal(_)))));
	assert!(matches!(data.get(8), Some(Value::Datetime(_))));
	assert!(matches!(data.get(9), Some(Value::Uuid(_))));
	let fallback = object(vec![("data", value), ("fixed", fixed)]);
	let result = parity(fallback.clone(), &opt, &ctx).await.unwrap();
	assert_eq!(format!("{result:?}"), format!("{fallback:?}"));
}

#[tokio::test]
async fn native_foreach_owned_element_retains_deferred_errors_and_return_boundary() {
	let (ctx, base) = mock().await;
	for futures in [false, true] {
		let opt = base.new_with_futures(futures);
		for value in [
			Value::parse("{value: <int>'7'}"),
			Value::parse("[string::uppercase('original'), $missing]"),
			Value::parse("{value: <future> { RETURN 17; }}"),
			Value::parse("{value: <future> { THROW 'original_foreach_future_error'; }}"),
			Value::parse("{value: { RETURN 19; }}"),
			Value::parse("{value: original:[1, 2] }"),
		] {
			let _ = parity(value, &opt, &ctx).await;
		}
	}
	let first = Value::parse("<future> { THROW 'original_foreach_first_error'; }");
	let too_deep = nested(64, true);
	let opt = base.clone().with_max_computation_depth(16).new_with_futures(true);
	let early = parity(object(vec![("a", first.clone()), ("z", too_deep.clone())]), &opt, &ctx)
		.await.unwrap_err().to_string();
	assert!(early.contains("original_foreach_first_error"), "{early}");
	let depth = parity(object(vec![("a", too_deep.clone()), ("z", first.clone())]), &opt, &ctx)
		.await.unwrap_err().to_string();
	assert!(depth.contains("Reached excessive computation depth"), "{depth}");
	let early = parity(array(vec![first.clone(), too_deep.clone()]), &opt, &ctx)
		.await.unwrap_err().to_string();
	assert!(early.contains("original_foreach_first_error"), "{early}");
	let depth = parity(array(vec![too_deep, first]), &opt, &ctx)
		.await.unwrap_err().to_string();
	assert!(depth.contains("Reached excessive computation depth"), "{depth}");
	// Generated identities cannot compare equal across independent calls. Both
	// must evaluate to a concrete UUID identity. The next control independently
	// checks exact evaluation counts through deterministic transaction effects.
	for original_path in [false, true] {
		let value = Value::parse("{id: original:uuid()}");
		let mut stack = reblessive::TreeStack::new();
		let result = stack.enter(|stk| async {
			if original_path { original(value, stk, &ctx, &base, None).await }
			else { compute_owned_iteration_value(value, stk, &ctx, &base, None).await }
		}).finish().await.unwrap();
		let Value::Object(fields) = result else { panic!("original object result"); };
		assert!(matches!(fields.get("id"), Some(Value::Thing(Thing { id: Id::Uuid(_), .. }))));
	}
}

#[tokio::test]
async fn native_foreach_owned_element_retains_deferred_evaluation_count_and_context() {
	// Each path uses a separate real native transaction. Unlike a FOR range,
	// this materialized object still contains the executable child when the
	// production operation and frozen original evaluator receive it.
	for original_path in [false, true] {
		let (ctx, opt) = mock().await;
		let opt = opt
			.with_ns(Some("foreach_element".into()))
			.with_db(Some("foreach_element".into()))
			.new_with_futures(true);
		let mut local = MutableContext::new(&ctx);
		local.add_value("parent_value", Value::from("inherited").into());
		let ctx = local.freeze();
		let setup = Value::parse("{ CREATE foreach_element:trace SET labels=[]; RETURN NONE; }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| setup.compute(stk, &ctx, &opt, None)).finish().await.unwrap();
		let value = object(vec![
			("a", Value::parse("{ UPDATE foreach_element:trace SET labels=array::append(labels, 'a') RETURN NONE; RETURN $parent_value; }")),
			("z", Value::parse("{ UPDATE foreach_element:trace SET labels=array::append(labels, 'z') RETURN NONE; RETURN 19; }")),
		]);
		let result = stack.enter(|stk| async {
			if original_path { original(value, stk, &ctx, &opt, None).await }
			else { compute_owned_iteration_value(value, stk, &ctx, &opt, None).await }
		}).finish().await.unwrap();
		let expected = object(vec![("a", "inherited".into()), ("z", 19_i64.into())]);
		assert_eq!(format!("{result:?}"), format!("{expected:?}"));
		let read = Value::parse("(SELECT VALUE labels FROM ONLY foreach_element:trace)");
		let trace = stack.enter(|stk| read.compute(stk, &ctx, &opt, None)).finish().await.unwrap();
		assert_eq!(trace, array(vec!["a".into(), "z".into()]));
	}
}
