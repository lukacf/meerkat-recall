//! Compare the missing-field shortcut with the retained native evaluator.
use super::*;
use crate::dbs::test::mock;
use crate::sql::{Idiom, Object, Param};

#[tokio::test]
async fn missing_object_field_preserves_native_tail_results_and_errors() {
	let (ctx, opt) = mock().await;
	let object = Value::Object(Object::default());
	let missing = Part::Field("missing".into());
	let mut stack = reblessive::tree::TreeStack::new();
	let cases = vec![
		vec![missing.clone()],
		vec![missing.clone(), Part::Field("child".into())],
		vec![missing.clone(), Part::Index(0.into())],
		vec![missing.clone(), Part::All],
		vec![missing.clone(), Part::Flatten],
		vec![missing.clone(), Part::Optional],
		vec![missing.clone(), Part::Method("is_none".into(), vec![])],
		vec![
			missing.clone(),
			Part::Field("child".into()),
			Part::Method("is_none".into(), vec![]),
		],
		vec![
			missing.clone(),
			Part::Optional,
			Part::Method("is_none".into(), vec![]),
		],
		vec![
			missing.clone(),
			Part::Method("absent_method".into(), vec![]),
		],
		vec![
			missing.clone(),
			Part::Method("is_none".into(), vec![Value::from(1)]),
		],
		// A dynamic index after NONE is skipped by the original next-method rule.
		vec![
			missing.clone(),
			Part::Value(Value::Param(Param::from("not_bound"))),
		],
		// Missing complex-id fields follow the same NONE tail as ordinary fields.
		vec![Part::Field("id".into()), Part::Field("child".into())],
	];
	for path in cases {
		let actual = stack
			.enter(|stk| object.get(stk, &ctx, &opt, None, &path))
			.finish()
			.await;
		let original = stack
			.enter(|stk| object.get_unoptimized(stk, &ctx, &opt, None, &path))
			.finish()
			.await;
		assert_eq!(
			format!("{actual:?}"),
			format!("{original:?}"),
			"path {path:?}"
		);
	}
	let path = [missing, Part::Method("is_none".into(), vec![])];
	assert_eq!(
		stack
			.enter(|stk| object.get(stk, &ctx, &opt, None, &path))
			.finish()
			.await
			.unwrap(),
		Value::Bool(true)
	);
}

#[tokio::test]
async fn missing_object_field_keeps_original_path_depth_refusal() {
	let (ctx, opt) = mock().await;
	let object = Value::Object(Object::default());
	let limit = usize::try_from(*MAX_COMPUTATION_DEPTH).unwrap();
	let path = Idiom(vec![Part::Field("absent".into()); limit + 1]);
	let mut stack = reblessive::tree::TreeStack::new();
	let actual = stack
		.enter(|stk| object.get(stk, &ctx, &opt, None, &path))
		.finish()
		.await;
	assert!(matches!(actual, Err(Error::ComputationDepthExceeded)));
}
