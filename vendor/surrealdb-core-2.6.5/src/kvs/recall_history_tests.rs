use super::*;
use crate::fnc::recall::recall_reference_identity_token;
use crate::sql::{Array, Dir, Object};

const JOURNAL: &str = "28a090cb-9741-43b0-8981-a9f030f6fc3c";
const KEY: [u8; 32] = [37; 32];

fn source() -> Thing {
	Thing::from(("source", "erased"))
}

fn unbound_authority(space: &str) -> RecallHistoryAuthority {
	let token = recall_reference_identity_token(JOURNAL, space, &KEY, &[], &source()).unwrap();
	RecallHistoryAuthority::new(
		"ns".into(),
		"db".into(),
		JOURNAL.into(),
		space.into(),
		KEY,
		vec![],
		[token].into_iter().collect(),
	)
	.unwrap()
}

fn authority(space: &str) -> RecallHistoryAuthority {
	unbound_authority(space)
		.bind_history("0".repeat(64), BTreeSet::new(), BTreeSet::new())
		.unwrap()
}

fn row(space: &str, source: Thing) -> Vec<u8> {
	let value = Value::Object(
		[
			("space_id".into(), Value::from(space)),
			("source_id".into(), Value::Thing(source)),
			("text".into(), Value::from("same independent text")),
		]
		.into_iter()
		.collect(),
	);
	revision::to_vec(&value).unwrap()
}

fn shared_key() -> Vec<u8> {
	record_key("ns", "db", "entity", &Id::String("shared".into())).unwrap()
}

#[test]
fn shared_history_keeps_b_before_and_after_source_bearing_version() {
	let authority = authority("household");
	let key = shared_key();
	let b = row("household", Thing::from(("source", "independent")));
	let a = row("household", source());
	assert_eq!(
		authority
			.classify_record_version(&key, &b, false, false, false)
			.unwrap(),
		RecordVersionDecision::Keep
	);
	assert_eq!(
		authority
			.classify_record_version(&key, &a, false, false, false)
			.unwrap(),
		RecordVersionDecision::EraseValue
	);
	assert_eq!(
		authority
			.classify_record_version(&key, &b, false, true, false)
			.unwrap(),
		RecordVersionDecision::Keep
	);
}

#[test]
fn historical_only_absent_row_still_requires_source_erasure() {
	let authority = authority("household");
	let key = record_key("ns", "db", "work_item", &Id::String("archived".into())).unwrap();
	assert_eq!(
		authority
			.classify_record_version(&key, &row("household", source()), false, false, false)
			.unwrap(),
		RecordVersionDecision::EraseValue
	);
	assert_eq!(
		authority
			.classify_record_version(&key, &[], true, true, false)
			.unwrap(),
		RecordVersionDecision::Keep
	);
}

#[test]
fn foreign_space_namespace_and_database_history_are_not_erased() {
	let authority = authority("household");
	let a = row("household", source());
	for (ns, db) in [("other", "db"), ("ns", "other")] {
		let key = record_key(ns, db, "entity", &Id::String("shared".into())).unwrap();
		assert_eq!(
			authority
				.classify_record_version(&key, &a, false, false, false)
				.unwrap(),
			RecordVersionDecision::Keep
		);
	}
	assert_eq!(
		authority
			.classify_record_version(
				&shared_key(),
				&row("business", source()),
				false,
				false,
				false
			)
			.unwrap(),
		RecordVersionDecision::Keep
	);
}

#[test]
fn current_source_bearing_payload_refuses_without_exact_control_verdict() {
	let authority = authority("household");
	let a = row("household", source());
	assert!(authority
		.classify_record_version(&shared_key(), &a, false, true, false)
		.is_err());
	assert_eq!(
		authority
			.classify_record_version(&shared_key(), &a, false, true, true)
			.unwrap(),
		RecordVersionDecision::Keep
	);
	assert_eq!(
		authority
			.classify_record_version(&shared_key(), &a, false, false, true)
			.unwrap(),
		RecordVersionDecision::Keep
	);
}

#[test]
fn malformed_and_trailing_native_values_refuse() {
	let authority = authority("household");
	let mut a = row("household", source());
	a.push(99);
	for value in [a, vec![255], revision::to_vec(&Value::from(3)).unwrap()] {
		assert!(authority
			.classify_record_version(&shared_key(), &value, false, false, false)
			.is_err());
	}
	assert!(authority
		.classify_record_version(&shared_key(), b"hidden", true, false, false)
		.is_err());
}

#[test]
fn unscoped_record_cannot_be_mistaken_for_independent_history() {
	let authority = authority("household");
	let value = Value::Object(
		[("source_id".into(), Value::Thing(source()))]
			.into_iter()
			.collect(),
	);
	assert_eq!(
		authority
			.classify_record_version(
				&shared_key(),
				&revision::to_vec(&value).unwrap(),
				false,
				false,
				false
			)
			.unwrap(),
		RecordVersionDecision::UnscopedRecord
	);
}

#[test]
fn strict_native_authority_round_trip_and_secret_redaction() {
	let authority = authority("household");
	let decoded = RecallHistoryAuthority::from_rpc_value(authority.to_rpc_value()).unwrap();
	assert_eq!(decoded.namespace(), "ns");
	assert_eq!(decoded.database(), "db");
	assert!(decoded.references_erased(&Value::Thing(source())).unwrap());
	let debug = format!("{decoded:?}");
	assert!(!debug.contains("37"));
	assert!(!debug.contains(JOURNAL));
	assert!(!debug.contains("membership:"));
}

#[test]
fn malformed_authority_never_echoes_private_values() {
	let authority = authority("household");
	for (name, replacement) in [
		("version", Value::from("1")),
		("key", Value::from("PRIVATE-KEY-MUST-NOT-APPEAR")),
		("key", Value::Bytes(vec![37; 31].into())),
		(
			"membership",
			Value::Array(vec![Value::from("PRIVATE-TOKEN-MUST-NOT-APPEAR")].into()),
		),
	] {
		let Value::Object(mut fields) = authority.to_rpc_value() else {
			unreachable!()
		};
		fields.insert(name.into(), replacement);
		let error = RecallHistoryAuthority::from_rpc_value(Value::Object(fields)).unwrap_err();
		assert!(!error.to_string().contains("PRIVATE"));
	}
	let Value::Object(mut fields) = authority.to_rpc_value() else {
		unreachable!()
	};
	fields.insert("extra".into(), Value::None);
	assert!(RecallHistoryAuthority::from_rpc_value(Value::Object(fields)).is_err());
}

#[test]
fn duplicate_membership_tokens_refuse() {
	let Value::Object(mut fields) = authority("household").to_rpc_value() else {
		unreachable!()
	};
	let Some(Value::Array(tokens)) = fields.get_mut("membership") else {
		unreachable!()
	};
	tokens.0.push(tokens.0[0].clone());
	assert!(RecallHistoryAuthority::from_rpc_value(Value::Object(fields)).is_err());
}

#[test]
fn exact_composite_record_identity_is_preserved() {
	let id = Id::Object(Object::from(
		[
			("number".into(), Value::from(42)),
			("source".into(), Value::Thing(source())),
		]
		.into_iter()
		.collect::<std::collections::BTreeMap<String, Value>>(),
	));
	let key = record_key("ns", "db", "extension", &id).unwrap();
	assert_eq!(
		decode_history_key(&key).unwrap(),
		NativeHistoryKey::Record {
			namespace: "ns".into(),
			database: "db".into(),
			identity: Thing::from(("extension", id)),
		}
	);
}

#[test]
fn normal_index_keys_expose_exact_fields_and_owner() {
	let fields = Array::from(vec![
		Value::from("source:erased is merely a substring"),
		Value::from("household"),
	]);
	let id = Id::String("shared".into());
	let bytes = key::index::Index::new("ns", "db", "entity", "name", &fields, Some(&id))
		.encode()
		.unwrap();
	assert_eq!(
		decode_history_key(&bytes).unwrap(),
		NativeHistoryKey::Index {
			namespace: "ns".into(),
			database: "db".into(),
			table: "entity".into(),
			index: "name".into(),
			fields,
			identity: Some(Thing::from(("entity", id))),
		}
	);
	let rid = Thing::from(("entity", "shared"));
	assert_eq!(
		unique_index_owner(&revision::to_vec(&rid).unwrap()).unwrap(),
		rid
	);
}

#[test]
fn aggregate_index_keys_are_not_classified_as_record_payloads() {
	let bytes = key::index::bu::Bu::new("ns", "db", "doc", "text", 9)
		.encode()
		.unwrap();
	assert_eq!(
		decode_history_key(&bytes).unwrap(),
		NativeHistoryKey::IndexInternal {
			namespace: "ns".into(),
			database: "db".into(),
			table: "doc".into(),
			index: "text".into(),
		}
	);
	assert_eq!(
		authority("household")
			.classify_record_version(&bytes, b"opaque node", false, false, false)
			.unwrap(),
		RecordVersionDecision::NotRecord
	);
}

#[test]
fn graph_and_reference_keys_keep_typed_endpoints() {
	let from = Thing::from(("entity", "shared"));
	let to = source();
	let graph = key::graph::Graph::new("ns", "db", &from.tb, from.id.clone(), Dir::Out, &to)
		.encode()
		.unwrap();
	assert_eq!(
		decode_history_key(&graph).unwrap(),
		NativeHistoryKey::Graph {
			namespace: "ns".into(),
			database: "db".into(),
			from: from.clone(),
			to: to.clone(),
		}
	);
	let reference = key::r#ref::Ref::new(
		"ns",
		"db",
		&from.tb,
		from.id.clone(),
		&to.tb,
		"source",
		to.id.clone(),
	)
	.encode()
	.unwrap();
	assert_eq!(
		decode_history_key(&reference).unwrap(),
		NativeHistoryKey::Reference {
			namespace: "ns".into(),
			database: "db".into(),
			from,
			to,
		}
	);
}

#[test]
fn malformed_native_key_is_never_a_keep_decision() {
	let mut key = shared_key();
	key.push(255);
	assert!(decode_history_key(&key).is_err());
	assert!(decode_history_key(b"/*ns\0*db\0*entity\0*\xff").is_err());
}

fn history_row(key: Vec<u8>, value: Vec<u8>, version: u64, current: bool) -> RecallHistoryRecord {
	RecallHistoryRecord {
		key,
		value,
		version,
		timestamp: version * 100,
		tombstone: false,
		current,
	}
}

fn snapshot(records: Vec<RecallHistoryRecord>) -> RecallHistorySnapshot {
	RecallHistorySnapshot::new("ns".into(), "db".into(), records).unwrap()
}

fn bound(
	snapshot: &RecallHistorySnapshot,
	erased: BTreeSet<String>,
	controls: BTreeSet<String>,
) -> RecallHistoryAuthority {
	unbound_authority("household")
		.bind_history(snapshot.fingerprint.clone(), erased, controls)
		.unwrap()
}

#[test]
fn selection_preserves_shared_b_versions_and_masks_erased_gap() {
	let key = shared_key();
	let b = row("household", Thing::from(("source", "independent")));
	let records = snapshot(vec![
		history_row(key.clone(), b.clone(), 1, false),
		history_row(key.clone(), row("household", source()), 2, false),
		history_row(key.clone(), b, 3, true),
	]);
	let selection = HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&key, 1, 100).unwrap(),
		HistoryDecision::Keep
	);
	assert_eq!(
		selection.decision(&key, 2, 200).unwrap(),
		HistoryDecision::Tombstone
	);
	assert_eq!(
		selection.decision(&key, 3, 300).unwrap(),
		HistoryDecision::Keep
	);
}

#[test]
fn all_source_owned_versions_and_deleted_identity_key_disappear() {
	let key = record_key("ns", "db", "work_item", &Id::String("archived".into())).unwrap();
	let deleted = RecallHistoryRecord {
		key: key.clone(),
		value: vec![],
		version: 3,
		timestamp: 300,
		tombstone: true,
		current: true,
	};
	let records = snapshot(vec![
		history_row(key.clone(), row("household", source()), 1, false),
		deleted,
	]);
	let selection = HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&key, 1, 100).unwrap(),
		HistoryDecision::Drop
	);
	assert_eq!(
		selection.decision(&key, 3, 300).unwrap(),
		HistoryDecision::Drop
	);
}

#[test]
fn causal_commitment_does_not_erase_identical_independent_earlier_version() {
	let key = shared_key();
	let independent = row("household", Thing::from(("source", "independent")));
	let a = history_row(key.clone(), independent.clone(), 2, false);
	let records = snapshot(vec![
		history_row(key.clone(), independent.clone(), 1, false),
		a.clone(),
		history_row(key.clone(), independent, 3, true),
	]);
	let token = unbound_authority("household")
		.version_commitment(&a)
		.unwrap();
	let selection = HistorySelection::prepare(
		&[bound(
			&records,
			[token].into_iter().collect(),
			BTreeSet::new(),
		)],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&key, 1, 100).unwrap(),
		HistoryDecision::Keep
	);
	assert_eq!(
		selection.decision(&key, 2, 200).unwrap(),
		HistoryDecision::Tombstone
	);
	assert_eq!(
		selection.decision(&key, 3, 300).unwrap(),
		HistoryDecision::Keep
	);
}

#[test]
fn native_version_commitment_binds_every_storage_component_and_custody() {
	let authority = unbound_authority("household");
	let original = history_row(shared_key(), row("household", source()), 1, false);
	let token = authority.version_commitment(&original).unwrap();
	let mut variants = vec![original.clone(); 6];
	variants[0].key.push(3);
	variants[1].value.push(4);
	variants[2].version += 1;
	variants[3].timestamp += 1;
	variants[4].current = true;
	variants[5].tombstone = true;
	for changed in variants {
		assert_ne!(authority.version_commitment(&changed).unwrap(), token);
	}
	assert_ne!(
		unbound_authority("business")
			.version_commitment(&original)
			.unwrap(),
		token
	);
	let mut rotated = authority.clone();
	rotated.key[0] ^= 1;
	assert_ne!(rotated.version_commitment(&original).unwrap(), token);
}

#[test]
fn inspected_stream_change_and_unlisted_version_refuse() {
	let key = shared_key();
	let records = snapshot(vec![history_row(
		key.clone(),
		row("household", Thing::from(("source", "independent"))),
		1,
		true,
	)]);
	let authority = bound(&records, BTreeSet::new(), BTreeSet::new());
	let selection = HistorySelection::prepare(&[authority.clone()], &records).unwrap();
	assert!(selection.decision(&key, 2, 200).is_err());
	let mut changed = records.clone();
	changed.records[0].timestamp += 1;
	assert!(HistorySelection::prepare(&[authority.clone()], &changed).is_err());
	let changed = snapshot(changed.records);
	assert!(HistorySelection::prepare(&[authority], &changed).is_err());
}

#[test]
fn scoped_snapshot_round_trip_preserves_full_unsigned_versions() {
	let mut row = history_row(
		shared_key(),
		row("household", Thing::from(("source", "independent"))),
		1,
		true,
	);
	row.version = u64::MAX - 1;
	row.timestamp = u64::MAX;
	let records = snapshot(vec![row]);
	let decoded = RecallHistorySnapshot::from_rpc_value(records.to_rpc_value()).unwrap();
	assert_eq!(decoded.records, records.records);
	assert_eq!(decoded.fingerprint, records.fingerprint);
	assert!(!format!("{decoded:?}").contains("same independent text"));
	assert!(!format!("{:?}", decoded.records[0]).contains("independent"));
}

#[test]
fn snapshot_rejects_duplicate_versions_foreign_keys_and_missing_current() {
	let row = history_row(
		shared_key(),
		row("household", Thing::from(("source", "independent"))),
		1,
		true,
	);
	assert!(
		RecallHistorySnapshot::new("ns".into(), "db".into(), vec![row.clone(), row.clone()])
			.is_err()
	);
	let mut foreign = row.clone();
	foreign.key = record_key("foreign", "db", "entity", &Id::String("shared".into())).unwrap();
	assert!(RecallHistorySnapshot::new("ns".into(), "db".into(), vec![foreign]).is_err());
	let mut no_current = row;
	no_current.current = false;
	assert!(RecallHistorySnapshot::new("ns".into(), "db".into(), vec![no_current]).is_err());
}

#[test]
fn current_control_requires_exact_native_version_proof() {
	let key = record_key("ns", "db", "source_head", &Id::String("minimal".into())).unwrap();
	let current = history_row(key.clone(), row("household", source()), 2, true);
	let records = snapshot(vec![current.clone()]);
	assert!(HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records
	)
	.is_err());
	let proof = unbound_authority("household")
		.version_commitment(&current)
		.unwrap();
	let selection = HistorySelection::prepare(
		&[bound(
			&records,
			BTreeSet::new(),
			[proof].into_iter().collect(),
		)],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&key, 2, 200).unwrap(),
		HistoryDecision::Keep
	);
}

fn scalar_definition(version: u64) -> RecallHistoryRecord {
	let definition = crate::sql::statements::DefineIndexStatement {
		name: "name".into(),
		what: "entity".into(),
		cols: crate::sql::Idioms(vec![crate::sql::Idiom::from("text")]),
		index: crate::sql::Index::Idx,
		defer: true,
		..Default::default()
	};
	history_row(
		key::table::ix::new("ns", "db", "entity", "name")
			.encode()
			.unwrap(),
		revision::to_vec(&definition).unwrap(),
		version,
		true,
	)
}

fn scalar_document(owner: Thing, text: &str, version: u64, current: bool) -> RecallHistoryRecord {
	let Value::Object(mut document) = decode_native::<Value>(&row("household", owner)).unwrap()
	else {
		panic!("object fixture")
	};
	document.insert("text".into(), Value::from(text));
	document.insert("id".into(), Value::Thing(Thing::from(("entity", "shared"))));
	history_row(
		shared_key(),
		encode_native_history_value(&Value::Object(document)).unwrap(),
		version,
		current,
	)
}

#[test]
fn source_index_deletion_uses_before_version_not_new_independent_afterimage() {
	let owner = Thing::from(("entity", "shared"));
	let fields = Array::from(vec![Value::from("source-exclusive indexed text")]);
	let index_key = key::index::Index::new("ns", "db", "entity", "name", &fields, Some(&owner.id))
		.encode()
		.unwrap();
	let records = snapshot(vec![
		scalar_definition(0),
		scalar_document(source(), "source-exclusive indexed text", 1, false),
		scalar_document(
			Thing::from(("source", "independent")),
			"independent replacement",
			2,
			true,
		),
		history_row(
			index_key.clone(),
			revision::to_vec(&owner).unwrap(),
			1,
			false,
		),
		RecallHistoryRecord {
			key: index_key.clone(),
			value: vec![],
			version: 2,
			timestamp: 200,
			tombstone: true,
			current: true,
		},
	]);
	let selection = HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&index_key, 1, 100).unwrap(),
		HistoryDecision::Drop
	);
	assert_eq!(
		selection.decision(&index_key, 2, 200).unwrap(),
		HistoryDecision::Drop
	);
}

#[test]
fn old_journal_proof_can_remain_after_its_original_version_is_gone() {
	let old = history_row(shared_key(), row("household", source()), 1, false);
	let proof = unbound_authority("household")
		.version_commitment(&old)
		.unwrap();
	let records = snapshot(vec![history_row(
		shared_key(),
		row("household", Thing::from(("source", "independent"))),
		2,
		true,
	)]);
	let selection = HistorySelection::prepare(
		&[bound(
			&records,
			[proof].into_iter().collect(),
			BTreeSet::new(),
		)],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&shared_key(), 2, 200).unwrap(),
		HistoryDecision::Keep
	);
}

#[test]
fn aggregate_prefixes_require_reconstruction_and_never_copy_old_nodes() {
	let key = key::index::bu::Bu::new("ns", "db", "doc", "text", 1)
		.encode()
		.unwrap();
	let records = snapshot(vec![history_row(
		key.clone(),
		b"old native term payload".to_vec(),
		1,
		true,
	)]);
	let selection = HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&key, 1, 100).unwrap(),
		HistoryDecision::Drop
	);
	assert_eq!(
		selection.aggregate_indexes(),
		&[("doc".into(), "text".into())].into_iter().collect()
	);
}

#[test]
fn unbound_authority_never_crosses_the_native_command_boundary() {
	let authority = unbound_authority("household");
	assert!(authority.validate_bound().is_err());
	assert!(RecallHistoryAuthority::from_rpc_value(authority.to_rpc_value()).is_err());
}

#[test]
fn versionless_deleted_index_owner_requires_exact_original_identity() {
	let source = source();
	let fields = Array::from(vec![Value::from("retired source text")]);
	let key = key::index::Index::new("ns", "db", "source", "text", &fields, Some(&source.id))
		.encode()
		.unwrap();
	let raw = RecallHistoryRecord {
		key: key.clone(),
		value: revision::to_vec(&source).unwrap(),
		version: 0,
		timestamp: 0,
		tombstone: false,
		current: true,
	};
	let records = snapshot(vec![raw]);
	let authority = bound(&records, BTreeSet::new(), BTreeSet::new());
	assert!(HistorySelection::prepare(&[authority.clone()], &records).is_err());
	let selection = HistorySelection::prepare_versionless(&[authority], &records).unwrap();
	assert_eq!(
		selection.decision(&key, 0, 0).unwrap(),
		HistoryDecision::Drop
	);
}

#[test]
fn versionless_mode_never_accepts_a_historical_snapshot() {
	let current = history_row(
		shared_key(),
		row("household", Thing::from(("source", "independent"))),
		2,
		true,
	);
	let old = history_row(shared_key(), current.value.clone(), 1, false);
	let records = snapshot(vec![old, current]);
	assert!(HistorySelection::prepare_versionless(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records
	)
	.is_err());
}

#[test]
fn bare_deferred_builder_marker_requires_reconstruction() {
	let key = key::index::all::new("ns", "db", "doc", "text")
		.encode()
		.unwrap();
	assert_eq!(key, b"/*ns\0*db\0*doc\0+text\0");
	assert_eq!(
		decode_history_key(&key).unwrap(),
		NativeHistoryKey::IndexInternal {
			namespace: "ns".into(),
			database: "db".into(),
			table: "doc".into(),
			index: "text".into(),
		}
	);
	let records = snapshot(vec![history_row(
		key.clone(),
		revision::to_vec(&false).unwrap(),
		1,
		true,
	)]);
	let selection = HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&key, 1, 100).unwrap(),
		HistoryDecision::Drop
	);
	assert_eq!(
		selection.aggregate_indexes(),
		&[("doc".into(), "text".into())].into_iter().collect()
	);
	for suffix in [b"\0".as_slice(), b"!", b"!bu", b"!zzopaque", b"\xff"] {
		let mut malformed = key.clone();
		malformed.extend_from_slice(suffix);
		assert!(decode_history_key(&malformed).is_err());
	}
	assert!(decode_history_key(&key[..key.len() - 1]).is_err());
}

#[test]
fn unknown_or_malformed_index_internal_key_is_never_a_drop_permission() {
	let mut unknown = key::index::all::new("ns", "db", "doc", "text")
		.encode()
		.unwrap();
	unknown.extend_from_slice(b"!zzopaque");
	assert!(decode_history_key(&unknown).is_err());
	let mut malformed = key::index::all::new("ns", "db", "doc", "text")
		.encode()
		.unwrap();
	malformed.extend_from_slice(b"!bu");
	assert!(decode_history_key(&malformed).is_err());
}

#[test]
fn original_selection_deadline_is_never_renewed() {
	let records = snapshot(vec![history_row(
		shared_key(),
		row("household", Thing::from(("source", "independent"))),
		1,
		true,
	)]);
	assert!(HistorySelection::prepare_with_deadline(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
		std::time::Instant::now()
	)
	.is_err());
}

#[test]
fn historical_system_metadata_needs_each_exact_inspected_version_proof() {
	let key = record_key(
		"ns",
		"db",
		"recall_durability",
		&Id::String("journal".into()),
	)
	.unwrap();
	let value = Value::Object(
		[
			(
				"id".into(),
				Value::Thing(Thing::from(("recall_durability", "journal"))),
			),
			("journal_id".into(), Value::from(JOURNAL)),
			(
				"reference_key_fingerprint".into(),
				Value::from("1".repeat(64)),
			),
		]
		.into_iter()
		.collect(),
	);
	let old = history_row(
		key.clone(),
		RecallHistoryRecord::encode_value(&value).unwrap(),
		1,
		false,
	);
	let current = history_row(key.clone(), old.value.clone(), 2, true);
	let records = snapshot(vec![old.clone(), current.clone()]);
	let authority = unbound_authority("household");
	let old_proof = authority.version_commitment(&old).unwrap();
	let current_proof = authority.version_commitment(&current).unwrap();
	assert!(HistorySelection::prepare(
		&[bound(
			&records,
			BTreeSet::new(),
			[current_proof.clone()].into_iter().collect()
		)],
		&records
	)
	.is_err());
	let selection = HistorySelection::prepare(
		&[bound(
			&records,
			BTreeSet::new(),
			[old_proof, current_proof].into_iter().collect(),
		)],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&key, 1, 100).unwrap(),
		HistoryDecision::Keep
	);
	assert_eq!(
		selection.decision(&key, 2, 200).unwrap(),
		HistoryDecision::Keep
	);
}

#[test]
fn public_native_encoder_preserves_exact_pinned_value_representation() {
	let value = Value::Array(
		vec![
			Value::None,
			Value::Null,
			Value::Thing(source()),
			Value::Bytes(vec![0, 255, 1].into()),
			Value::from(42_i64),
			Value::from("42"),
		]
		.into(),
	);
	let encoded = encode_native_history_value(&value).unwrap();
	assert_eq!(encoded, revision::to_vec(&value).unwrap());
	assert_eq!(decode_native::<Value>(&encoded).unwrap(), value);
}

#[test]
fn tombstone_only_exact_erased_identity_is_removed_but_independent_identity_stays() {
	let removed = record_key("ns", "db", "source", &source().id).unwrap();
	let independent = record_key("ns", "db", "source", &Id::String("independent".into())).unwrap();
	let rows = [removed.clone(), independent.clone()]
		.into_iter()
		.map(|key| RecallHistoryRecord {
			key,
			value: vec![],
			version: 9,
			timestamp: 900,
			tombstone: true,
			current: true,
		})
		.collect();
	let records = snapshot(rows);
	let authority = bound(&records, BTreeSet::new(), BTreeSet::new());
	for selection in [
		HistorySelection::prepare(&[authority.clone()], &records).unwrap(),
		HistorySelection::prepare_versionless(&[authority], &records).unwrap(),
	] {
		assert_eq!(
			selection.decision(&removed, 9, 900).unwrap(),
			HistoryDecision::Drop
		);
		assert_eq!(
			selection.decision(&independent, 9, 900).unwrap(),
			HistoryDecision::Keep
		);
	}
}

#[test]
fn nonversioned_store_retains_actual_native_transaction_clock() {
	let records = snapshot(vec![history_row(
		shared_key(),
		row("household", Thing::from(("source", "independent"))),
		9,
		true,
	)]);
	let selection = HistorySelection::prepare_versionless(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	assert_eq!(
		selection.decision(&shared_key(), 9, 900).unwrap(),
		HistoryDecision::Keep
	);
}

#[test]
fn backup_origin_mac_binds_native_content_scope_and_original_key() {
	let key = shared_key();
	let value = row("household", source());
	let original =
		backup_origin_content_commitment(JOURNAL, "ns", "db", &KEY, &key, &value).unwrap();
	let mut changed = value.clone();
	changed.push(9);
	assert_ne!(
		backup_origin_content_commitment(JOURNAL, "ns", "db", &KEY, &key, &changed).unwrap(),
		original
	);
	assert_ne!(
		backup_origin_content_commitment(JOURNAL, "ns", "db", &[38; 32], &key, &value).unwrap(),
		original
	);
	assert!(
		backup_origin_content_commitment(JOURNAL, "different", "db", &KEY, &key, &value).is_err()
	);
	let metadata = b"strict canonical backup sidecar bytes";
	let auth = backup_origin_authenticator(JOURNAL, "ns", "db", &KEY, metadata).unwrap();
	assert_ne!(
		auth,
		backup_origin_authenticator(JOURNAL, "ns", "other", &KEY, metadata).unwrap()
	);
	assert_ne!(
		auth,
		backup_origin_authenticator(JOURNAL, "ns", "db", &[38; 32], metadata).unwrap()
	);
	assert_ne!(auth, original);
}

#[test]
fn versionless_independent_scalar_index_is_rebuilt_but_versioned_history_stays() {
	let owner = Thing::from(("entity", "shared"));
	let fields = Array::from(vec![Value::from("independent field")]);
	let key = key::index::Index::new("ns", "db", "entity", "name", &fields, Some(&owner.id))
		.encode()
		.unwrap();
	let document = scalar_document(
		Thing::from(("source", "independent")),
		"independent field",
		17,
		true,
	);
	let index = history_row(key.clone(), revision::to_vec(&owner).unwrap(), 17, true);
	let records = snapshot(vec![scalar_definition(0), document, index]);
	let authority = bound(&records, BTreeSet::new(), BTreeSet::new());
	let versioned = HistorySelection::prepare(&[authority.clone()], &records).unwrap();
	assert_eq!(
		versioned.decision(&key, 17, 1700).unwrap(),
		HistoryDecision::Keep
	);
	assert!(versioned.aggregate_indexes().is_empty());
	let versionless = HistorySelection::prepare_versionless(&[authority], &records).unwrap();
	assert_eq!(
		versionless.decision(&key, 17, 1700).unwrap(),
		HistoryDecision::Drop
	);
	assert_eq!(
		versionless.aggregate_indexes(),
		&[("entity".into(), "name".into())].into_iter().collect()
	);
}

#[test]
fn known_metadata_codecs_roundtrip_and_unknown_scope_keys_refuse() {
	let keys = vec![
		key::database::ac::new("ns", "db", "access")
			.encode()
			.unwrap(),
		key::database::ap::new("ns", "db", "/api").encode().unwrap(),
		key::database::az::new("ns", "db", "analyzer")
			.encode()
			.unwrap(),
		key::database::cg::new("ns", "db", "graphql")
			.encode()
			.unwrap(),
		key::database::fc::new("ns", "db", "function")
			.encode()
			.unwrap(),
		key::database::ml::new("ns", "db", "model", "1")
			.encode()
			.unwrap(),
		key::database::pa::new("ns", "db", "parameter")
			.encode()
			.unwrap(),
		key::database::tb::new("ns", "db", "entity")
			.encode()
			.unwrap(),
		key::database::ts::new("ns", "db", 17).encode().unwrap(),
		key::database::us::new("ns", "db", "owner")
			.encode()
			.unwrap(),
		key::database::vs::new("ns", "db").encode().unwrap(),
		key::database::access::gr::new("ns", "db", "method", "grant")
			.encode()
			.unwrap(),
		key::table::ev::new("ns", "db", "entity", "event")
			.encode()
			.unwrap(),
		key::table::fd::new("ns", "db", "entity", "field")
			.encode()
			.unwrap(),
		key::table::ft::new("ns", "db", "entity", "view")
			.encode()
			.unwrap(),
		key::table::ix::new("ns", "db", "entity", "index")
			.encode()
			.unwrap(),
		key::table::lq::new("ns", "db", "entity", uuid::Uuid::nil())
			.encode()
			.unwrap(),
	];
	for key in keys {
		assert_eq!(decode_history_key(&key).unwrap(), NativeHistoryKey::Other);
		let mut corrupt = key.clone();
		corrupt.push(255);
		assert!(decode_history_key(&corrupt).is_err());
	}
	for key in [
		b"/*ns\0*db\0*entity\0?unknown".as_slice(),
		b"/*ns\0*db\0!zzopaque\0",
	] {
		assert!(decode_history_key(key).is_err());
	}
}

#[test]
fn delayed_erased_scalar_tuple_after_independent_record_never_inherits_retention() {
	let owner = Thing::from(("entity", "shared"));
	let erased_key = ordinary_index_key(
		"ns",
		"db",
		"entity",
		"name",
		&Array::from(vec![Value::from("erased private tuple")]),
		Some(&owner.id),
	)
	.unwrap();
	let kept_key = ordinary_index_key(
		"ns",
		"db",
		"entity",
		"name",
		&Array::from(vec![Value::from("independent replacement")]),
		Some(&owner.id),
	)
	.unwrap();
	let records = snapshot(vec![
		scalar_definition(0),
		scalar_document(source(), "erased private tuple", 1, false),
		scalar_document(
			Thing::from(("source", "independent")),
			"independent replacement",
			2,
			true,
		),
		// A's captured fields arrive after B's replacement, as in IndexBuilder.
		history_row(
			erased_key.clone(),
			revision::to_vec(&owner).unwrap(),
			3,
			true,
		),
		history_row(kept_key.clone(), revision::to_vec(&owner).unwrap(), 2, true),
	]);
	let selected = HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	assert_eq!(
		selected.decision(&erased_key, 3, 300).unwrap(),
		HistoryDecision::Drop
	);
	assert_eq!(
		selected.decision(&kept_key, 2, 200).unwrap(),
		HistoryDecision::Keep
	);
	assert_eq!(
		selected.decision(&shared_key(), 2, 200).unwrap(),
		HistoryDecision::Keep
	);
	assert!(selected
		.aggregate_indexes()
		.contains(&("entity".into(), "name".into())));
}

#[test]
fn independent_value_before_index_definition_is_not_a_scalar_witness() {
	let owner = Thing::from(("entity", "shared"));
	let index_key = ordinary_index_key(
		"ns",
		"db",
		"entity",
		"name",
		&Array::from(vec![Value::from("shared exact tuple")]),
		Some(&owner.id),
	)
	.unwrap();
	let records = snapshot(vec![
		scalar_document(
			Thing::from(("source", "independent")),
			"shared exact tuple",
			1,
			false,
		),
		scalar_document(source(), "shared exact tuple", 2, false),
		scalar_definition(3),
		scalar_document(
			Thing::from(("source", "independent")),
			"new retained tuple",
			4,
			true,
		),
		history_row(
			index_key.clone(),
			revision::to_vec(&owner).unwrap(),
			5,
			true,
		),
	]);
	let selected = HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	assert_eq!(
		selected.decision(&index_key, 5, 500).unwrap(),
		HistoryDecision::Drop
	);
	assert_eq!(
		selected.decision(&shared_key(), 1, 100).unwrap(),
		HistoryDecision::Keep
	);
}

#[test]
fn scalar_tuple_without_exact_document_or_definition_evidence_refuses() {
	let owner = Thing::from(("entity", "shared"));
	let index_key = ordinary_index_key(
		"ns",
		"db",
		"entity",
		"name",
		&Array::from(vec![Value::from("unproven tuple")]),
		Some(&owner.id),
	)
	.unwrap();
	for include_definition in [false, true] {
		let mut rows = vec![
			scalar_document(
				Thing::from(("source", "independent")),
				"actual independent value",
				1,
				true,
			),
			history_row(
				index_key.clone(),
				revision::to_vec(&owner).unwrap(),
				2,
				true,
			),
		];
		if include_definition {
			rows.push(scalar_definition(0));
		}
		let records = snapshot(rows);
		assert!(HistorySelection::prepare(
			&[bound(&records, BTreeSet::new(), BTreeSet::new())],
			&records
		)
		.is_err());
	}
}

#[test]
fn future_independent_scalar_tuple_cannot_authorize_an_earlier_erased_version() {
	let owner = Thing::from(("entity", "shared"));
	let key = ordinary_index_key(
		"ns",
		"db",
		"entity",
		"name",
		&Array::from(vec![Value::from("identical independent tuple")]),
		Some(&owner.id),
	)
	.unwrap();
	let records = snapshot(vec![
		scalar_definition(0),
		scalar_document(source(), "identical independent tuple", 1, false),
		scalar_document(
			Thing::from(("source", "independent")),
			"identical independent tuple",
			3,
			true,
		),
		history_row(key.clone(), revision::to_vec(&owner).unwrap(), 2, false),
		history_row(key.clone(), revision::to_vec(&owner).unwrap(), 3, true),
	]);
	let selected = HistorySelection::prepare(
		&[bound(&records, BTreeSet::new(), BTreeSet::new())],
		&records,
	)
	.unwrap();
	// Same field bytes and owner do not transfer future B authority backward.
	// Preserve the native absence boundary while retaining B's later value.
	assert_eq!(
		selected.decision(&key, 2, 200).unwrap(),
		HistoryDecision::Tombstone
	);
	assert_eq!(
		selected.decision(&key, 3, 300).unwrap(),
		HistoryDecision::Keep
	);
	assert_eq!(
		selected.decision(&shared_key(), 3, 300).unwrap(),
		HistoryDecision::Keep
	);
}

#[test]
fn full_text_global_state_uses_actual_table_local_writer_codec() {
	for index in ["search", "second_search"] {
		let key = key::index::bs::Bs::new("ns", "db", "entity", index)
			.encode()
			.unwrap();
		let mut expected = b"/*ns\0*db\0*entity\0!bs".to_vec();
		expected.extend_from_slice(index.as_bytes());
		expected.push(0);
		assert_eq!(key, expected);
		assert_eq!(
			decode_history_key(&key).unwrap(),
			NativeHistoryKey::IndexInternal {
				namespace: "ns".into(),
				database: "db".into(),
				table: "entity".into(),
				index: index.into(),
			}
		);
	}
}

#[test]
fn malformed_or_relocated_full_text_state_never_authorizes_copy_or_drop() {
	for key in [
		b"/*ns\0*db\0*entity\0!bssearch".as_slice(),
		b"/*ns\0*db\0*entity\0!bssearch\0\xff",
		b"/*ns\0*db\0*entity\0+search\0!bs",
		b"/*ns\0*db\0*entity\0+search\0!bssearch\0",
		b"/*ns\0*db\0*entity\0!bzsearch\0",
	] {
		assert!(decode_history_key(key).is_err());
	}
}
