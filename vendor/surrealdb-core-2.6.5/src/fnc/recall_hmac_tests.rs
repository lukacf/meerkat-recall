//! The original per-identity HMAC algorithm remains a separate test oracle.
#![allow(clippy::unwrap_used)]

use super::*;

const JOURNAL: &str = "28a090cb-9741-43b0-8981-a9f030f6fc3c";
const KEY: [u8; 32] = [37; 32];

// Retained pre-optimization algorithm: assemble the WHOLE original message,
// initialize a fresh HMAC key, and update it once. It does not clone or read
// the candidate's prefixed HMAC state. The typed byte encoder is unchanged.
fn original_identity_token(
	reference: &Thing,
	journal: &str,
	space: &str,
	key: &[u8; 32],
	typed_tables: &BTreeSet<&str>,
) -> Result<String, Error> {
	if reference.tb.is_empty() {
		return Err(invalid("empty record table is unsupported"));
	}
	let mut message = DOMAIN.to_vec();
	field(&mut message, journal.as_bytes());
	field(&mut message, space.as_bytes());
	field(&mut message, reference.tb.as_bytes());
	let normalized = match &reference.id {
		Id::String(value) if typed_tables.contains(reference.tb.as_str()) => {
			uuid::Uuid::parse_str(value)
				.ok()
				.map(|uuid| Id::Uuid(uuid.into()))
		}
		_ => None,
	};
	encode_id(
		&mut message,
		normalized.as_ref().unwrap_or(&reference.id),
		0,
	)?;
	let mut mac =
		Hmac::<Sha256>::new_from_slice(key).map_err(|_| invalid("invalid reference key"))?;
	mac.update(&message);
	Ok(hex::encode(mac.finalize().into_bytes()))
}

#[test]
fn prefixed_hmac_matches_original_whole_message_without_cross_identity_state() {
	let tables = vec!["source".to_owned()];
	let typed = BTreeSet::from(["source"]);
	let uuid = uuid::Uuid::parse_str("8b12ceab-6f6a-4b12-8438-b31a56a3ddbb").unwrap();
	let references = [
		Thing::from(("source", "alpha")),
		Thing::from(("source", Id::Number(-7))),
		Thing::from(("source", "-7")),
		Thing::from(("source", Id::String(uuid.to_string()))),
		Thing::from(("source", Id::Uuid(uuid.into()))),
		Thing::from(("extension", Id::String(uuid.to_string()))),
		Thing::from((
			"edge",
			Id::Array(vec![Value::from("x"), Value::from(7_i64)].into()),
		)),
		Thing::from((
			"edge",
			Id::Object(
				std::collections::BTreeMap::<String, Value>::from([
					(
						"link".into(),
						Value::Thing(Thing::from(("source", "alpha"))),
					),
					("nothing".into(), Value::None),
				])
				.into(),
			),
		)),
		Thing::from(("", "invalid")),
		Thing::from((
			"edge",
			Id::Array(vec![Value::Param(crate::sql::Param::from("deferred"))].into()),
		)),
	];
	for (journal, space, key) in [
		(JOURNAL, "home", KEY),
		(JOURNAL, "other-space", KEY),
		(JOURNAL, "home", [38; 32]),
		("38a090cb-9741-43b0-8981-a9f030f6fc3c", "home", KEY),
	] {
		let candidate = extractor(journal, space, &key, &tables).unwrap();
		// Reverse and repeat the same references after intervening failures:
		// neither a successful token nor an invalid ID may mutate the prefix.
		for reference in references
			.iter()
			.chain(references.iter().rev())
			.chain(&references)
		{
			let actual = candidate
				.identity_token(reference)
				.map_err(|error| error.to_string());
			let expected = original_identity_token(reference, journal, space, &key, &typed)
				.map_err(|error| error.to_string());
			assert_eq!(actual, expected);
		}
	}
}

#[test]
fn prefixed_hmac_does_not_skip_duplicate_visits_or_reference_limits() {
	let tables = Vec::new();
	let reference = Value::Thing(Thing::from(("source", "alpha")));
	let value = Value::Array(vec![reference.clone(), reference].into());
	let mut exact = extractor(JOURNAL, "home", &KEY, &tables).unwrap();
	exact.remaining_nodes = 5; // array + (native Thing + reference) twice
	exact.native(&value, 0).unwrap();
	assert_eq!(exact.remaining_nodes, 0);
	assert_eq!(exact.tokens.len(), 1);
	let mut limited = extractor(JOURNAL, "home", &KEY, &tables).unwrap();
	limited.remaining_nodes = 4;
	assert_eq!(
		limited.native(&value, 0).unwrap_err().to_string(),
		invalid("reference node limit exceeded").to_string()
	);
	let mut depth = extractor(JOURNAL, "home", &KEY, &tables).unwrap();
	assert_eq!(
		depth.native(&value, MAX_DEPTH + 1).unwrap_err().to_string(),
		invalid("reference nesting limit exceeded").to_string()
	);
	let mut decoded = extractor(JOURNAL, "home", &KEY, &tables).unwrap();
	decoded.decoded_bytes = MAX_DECODED_BYTES;
	assert_eq!(
		decoded
			.string("\"source:alpha\"", 0)
			.unwrap_err()
			.to_string(),
		invalid("encoded reference byte limit exceeded").to_string()
	);
}
