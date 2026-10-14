//! Frozen pre-change dispatcher is an independent size and depth/error oracle.
use super::*;
use crate::dbs::test::mock;
use crate::syn::Parse;

// Exact f6de8ae dispatcher. Keep its four direct awaits: they are the allocation
// regression this control measures, not an alternate production entry point.
async fn original(
	value: &Value,
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value, Error> {
	let opt = &opt.dive(1)?;
	match value {
		Value::Cast(v) => v.compute(stk, ctx, opt, doc).await,
		Value::Thing(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Block(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Range(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Param(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Idiom(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Array(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Object(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Future(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Constant(v) => v.compute(),
		Value::Function(v) => v.compute(stk, ctx, opt, doc).await,
		Value::Model(v) => v.compute(stk, ctx, opt, doc).await,
		Value::Subquery(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Expression(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Edges(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
		Value::Refs(v) => v.compute(ctx, opt, doc).await,
		_ => Ok(value.to_owned()),
	}
}

#[tokio::test]
async fn native_value_dispatch_future_is_smaller_than_original_inline_dispatch() {
	let (ctx, opt) = mock().await;
	let value = Value::from(7);
	let mut stack = reblessive::TreeStack::new();
	stack
		.enter(|stk| async move {
			let before = std::mem::size_of_val(&original(&value, stk, &ctx, &opt, None));
			let after = std::mem::size_of_val(&value.compute_unbordered(stk, &ctx, &opt, None));
			assert!(
				after < before,
				"dispatch state machine did not shrink: {before} -> {after}"
			);
		})
		.finish()
		.await;
}

#[tokio::test]
async fn native_value_dispatch_keeps_original_depth_and_first_error() {
	let (ctx, opt) = mock().await;
	let Value::Array(scalars) = Value::parse("[NONE, NULL, true, 17, 1.25f, 1.250dec, 'text', 7s, d'2025-01-02T03:04:05Z', u'12345678-1234-5678-9234-567812345678', original:7, original:fixed]") else {
		panic!("fixed native scalar fixture");
	};
	let mut values = scalars.0;
	values.extend([
		Value::Thing(Thing::from((
			"original",
			Id::Uuid(crate::sql::Uuid::from(
				uuid::Uuid::parse_str("12345678-1234-5678-9234-567812345678").unwrap(),
			)),
		))),
		Value::Bytes(vec![0_u8, 1, 255].into()),
		Value::Geometry((1.25_f64, 2.5_f64).into()),
		Value::Regex("^(a|b)$".parse().unwrap()),
	]);
	// Explicit independent expectation: every scalar still consumes exactly one
	// level, without changing the caller's budget or its native representation.
	for value in &values {
		for depth in [0, 1, 2] {
			let opt = opt.clone().with_max_computation_depth(depth);
			let mut stack = reblessive::TreeStack::new();
			let result = stack
				.enter(|stk| value.compute_unbordered(stk, &ctx, &opt, None))
				.finish()
				.await;
			if depth == 0 {
				assert!(matches!(result, Err(Error::ComputationDepthExceeded)));
			} else {
				assert_eq!(format!("{:?}", result.unwrap()), format!("{value:?}"));
				assert!(opt.dive(depth as u8).is_ok(), "caller budget changed");
			}
		}
	}
	values.extend([
		Value::None,
		Value::parse("original:[7, 'fixed']"),
		Value::parse("original:{key: 7}"),
		Value::parse("[NONE, {child: 7}]"),
		Value::parse("<int>'7'"),
		Value::parse("<int>'wrong'"),
		Value::parse("string::uppercase('guard')"),
		Value::parse("array::fold([1, 2], 0, |$sum, $value| $sum + $value)"),
		Value::parse("{ RETURN 17; }"),
		Value::parse("<future> { THROW 'dispatch_original_first_error'; }"),
		Value::Refs(crate::sql::reference::Refs(Vec::new())),
		// No model is installed or invoked: missing database options (or the
		// disabled ML feature) refuse before any model definition/file access.
		Value::Model(Box::new(crate::sql::Model::default())),
	]);
	for value in values {
		for depth in 0..=24 {
			for futures in [false, true] {
				let opt = opt
					.clone()
					.with_max_computation_depth(depth)
					.new_with_futures(futures);
				let mut stack = reblessive::TreeStack::new();
				let before = stack
					.enter(|stk| original(&value, stk, &ctx, &opt, None))
					.finish()
					.await;
				let after = stack
					.enter(|stk| value.compute_unbordered(stk, &ctx, &opt, None))
					.finish()
					.await;
				assert_eq!(
					format!("{after:?}"),
					format!("{before:?}"),
					"{value}, depth={depth}, futures={futures}"
				);
			}
		}
	}
}

#[tokio::test]
async fn native_value_dispatch_preserves_return_boundary() {
	let (ctx, opt) = mock().await;
	let value = Value::parse("{ RETURN <int>'23'; }");
	let mut stack = reblessive::TreeStack::new();
	let before = stack
		.enter(|stk| original(&value, stk, &ctx, &opt, None))
		.finish()
		.await;
	assert!(matches!(before, Err(Error::Return { value }) if value == Value::from(23)));
	let after = stack
		.enter(|stk| value.compute(stk, &ctx, &opt, None))
		.finish()
		.await
		.unwrap();
	assert_eq!(after, Value::from(23));
}
