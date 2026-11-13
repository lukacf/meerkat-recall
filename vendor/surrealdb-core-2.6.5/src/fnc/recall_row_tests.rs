//! The retained owned-array API is the differential oracle for borrowed rows.
#![allow(clippy::unwrap_used)]

use super::*;

const JOURNAL: &str = "28a090cb-9741-43b0-8981-a9f030f6fc3c";
const KEY: [u8; 32] = [37; 32];

fn equivalent(identity: &Thing, row: &Value) {
	let tables = vec!["source".to_owned()];
	for (space, key) in [("home", KEY), ("other", [38; 32])] {
		let expected = recall_reference_tokens(
			JOURNAL,
			space,
			&key,
			&tables,
			&Value::Array(vec![Value::Thing(identity.clone()), row.clone()].into()),
		)
		.map_err(|error| error.to_string());
		let actual = recall_reference_row_tokens(JOURNAL, space, &key, &tables, identity, row)
			.map_err(|error| error.to_string());
		assert_eq!(actual, expected);
	}
}

#[test]
fn borrowed_rows_preserve_native_serialized_and_composite_references() {
	let source = Thing::from(("source", "8b12ceab-6f6a-4b12-8438-b31a56a3ddbb"));
	let row = Value::Object(
		std::collections::BTreeMap::<String, Value>::from([
			("native".into(), Value::Thing(source.clone())),
			("source:legacy-key".into(), Value::None),
			(
				"encoded".into(),
				Value::from(r#"{"nested":["source:encoded",{"tb":"source","id":"json-id"}]}"#),
			),
			(
				"quoted".into(),
				Value::from(Thing::from(("odd table", "odd key")).to_string()),
			),
			(
				"prose".into(),
				Value::from("text mentions source:ignored and https://example.test"),
			),
			(
				"numeric".into(),
				Value::Thing(Thing::from(("extension", Id::Number(17)))),
			),
			(
				"uuid".into(),
				Value::Thing(Thing::from((
					"extension",
					Id::Uuid(uuid::Uuid::nil().into()),
				))),
			),
			(
				"object".into(),
				Value::Object(
					std::collections::BTreeMap::<String, Value>::from([
						("tb".into(), Value::from("source")),
						("id".into(), Value::from("object-id")),
					])
					.into(),
				),
			),
		])
		.into(),
	);
	for identity in [
		source.clone(),
		Thing::from((
			"extension",
			Id::Array(vec![Value::Thing(source), Value::from(42)].into()),
		)),
	] {
		equivalent(&identity, &row);
		equivalent(&identity, &Value::None);
		equivalent(&identity, &Value::Null);
	}
}

#[test]
fn borrowed_rows_preserve_failures_and_nesting_boundary() {
	let identity = Thing::from(("source", "root"));
	equivalent(&Thing::from(("", "invalid")), &Value::None);
	equivalent(
		&identity,
		&Value::Param(crate::sql::Param::from("unevaluated")),
	);
	let mut row = Value::None;
	for _ in 0..=MAX_DEPTH + 1 {
		equivalent(&identity, &row);
		row = Value::Array(vec![row].into());
	}
}

#[test]
fn borrowed_rows_share_exact_node_and_decoded_byte_budgets() {
	let identity = Thing::from(("source", "root"));
	let row = Value::from(r#"["source:encoded", "source:encoded"]"#);
	let owned = Value::Array(vec![Value::Thing(identity.clone()), row.clone()].into());
	for nodes in 0..30 {
		for bytes in [0, 1, MAX_DECODED_BYTES - 1, MAX_DECODED_BYTES] {
			let mut old = extractor(JOURNAL, "home", &KEY, &[]).unwrap();
			let mut new = extractor(JOURNAL, "home", &KEY, &[]).unwrap();
			old.remaining_nodes = nodes;
			new.remaining_nodes = nodes;
			old.decoded_bytes = bytes;
			new.decoded_bytes = bytes;
			assert_eq!(
				new.row(&identity, &row).map_err(|e| e.to_string()),
				old.native(&owned, 0).map_err(|e| e.to_string())
			);
			assert_eq!(new.remaining_nodes, old.remaining_nodes);
			assert_eq!(new.decoded_bytes, old.decoded_bytes);
			assert_eq!(new.tokens, old.tokens);
		}
	}
}

#[test]
fn borrowed_row_identity_is_not_lost_and_composite_references_remain_fenced() {
	let source = Thing::from(("source", "original"));
	let identity = Thing::from((
		"extension",
		Id::Array(vec![Value::Thing(source.clone())].into()),
	));
	let tokens =
		recall_reference_row_tokens(JOURNAL, "home", &KEY, &[], &identity, &Value::None).unwrap();
	assert!(tokens.contains(
		&recall_reference_identity_token(JOURNAL, "home", &KEY, &[], &identity).unwrap()
	));
	assert!(tokens
		.contains(&recall_reference_identity_token(JOURNAL, "home", &KEY, &[], &source).unwrap()));
	assert_eq!(tokens.len(), 2);
}

#[test]
fn concurrent_borrowed_rows_keep_input_and_invocation_state_independent() {
	let identity = Thing::from(("source", "physical-row"));
	let row = Value::Array(
		(0..32)
			.map(|i| Value::from(Thing::from(("odd table", Id::String(format!("key {i}")))).to_string()))
			.collect::<Vec<_>>()
			.into(),
	);
	let before = row.clone();
	std::thread::scope(|scope| {
		for _ in 0..4 {
			let identity = &identity;
			let row = &row;
			scope.spawn(move || {
				for _ in 0..32 {
					equivalent(identity, row);
				}
			});
		}
	});
	assert_eq!(row, before);
}
