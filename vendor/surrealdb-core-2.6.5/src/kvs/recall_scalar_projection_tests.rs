use super::*;
use crate::kvs::{KeyDecode, KeyEncode};
use crate::sql::statements::DefineStatement;
use crate::sql::{Array, Statement};
use std::time::Duration;

fn definition(sql: &str) -> DefineIndexStatement {
    let query = crate::syn::parse(sql).unwrap();
    let Statement::Define(DefineStatement::Index(index)) = &query.0 .0[0] else {
        panic!("expected a native index definition");
    };
    index.clone()
}

fn fields(keys: BTreeSet<Vec<u8>>) -> Vec<Array> {
    keys.into_iter()
        .map(|key| {
            crate::key::index::Index::decode(&key)
                .unwrap()
                .fd
                .into_owned()
        })
        .collect()
}

#[test]
fn native_projection_matches_cartesian_array_and_unique_key_encodings() {
    let owner = Thing::from(("corpus", "b"));
    let document = crate::syn::value("{id:corpus:b,a:['one','two'],b:[10,20]}").unwrap();
    let index = definition("DEFINE INDEX tuple_idx ON corpus FIELDS a,b");
    let keys = scalar_record_keys("ns", "db", &index, &owner, &document, None).unwrap();
    let mut expected = BTreeSet::new();
    for a in ["one", "two"] {
        for b in [10, 20] {
            let fields = Array::from(vec![Value::from(a), Value::from(b)]);
            expected.insert(
                crate::key::index::Index::new(
                    "ns",
                    "db",
                    "corpus",
                    "tuple_idx",
                    &fields,
                    Some(&owner.id),
                )
                .encode()
                .unwrap(),
            );
        }
    }
    assert_eq!(keys, expected);
    let unique = definition("DEFINE INDEX tuple_idx ON corpus FIELDS a,b UNIQUE");
    let keys = scalar_record_keys("ns", "db", &unique, &owner, &document, None).unwrap();
    assert_eq!(keys.len(), 4);
    assert!(keys
        .iter()
        .all(|key| crate::key::index::Index::decode(key).unwrap().id.is_none()));
}

#[test]
fn native_projection_preserves_flatten_empty_and_unique_null_semantics() {
    let owner = Thing::from(("corpus", "b"));
    let document = crate::syn::value("{id:corpus:b,a:['one','two']}").unwrap();
    let flattened = definition("DEFINE INDEX tuple_idx ON corpus FIELDS a...");
    assert_eq!(
        fields(scalar_record_keys("ns", "db", &flattened, &owner, &document, None).unwrap()),
        vec![Array::from(vec![Value::Array(
            vec![Value::from("one"), Value::from("two")].into()
        )])]
    );
    let unique = definition("DEFINE INDEX tuple_idx ON corpus FIELDS missing UNIQUE");
    assert!(
        scalar_record_keys("ns", "db", &unique, &owner, &document, None)
            .unwrap()
            .is_empty()
    );
    let ordinary = definition("DEFINE INDEX tuple_idx ON corpus FIELDS missing");
    assert_eq!(
        fields(scalar_record_keys("ns", "db", &ordinary, &owner, &document, None).unwrap()),
        vec![Array::from(vec![Value::None])]
    );
}

#[test]
fn native_projection_uses_nested_local_values_without_dereferencing_records() {
    let owner = Thing::from(("corpus", "b"));
    let document =
        crate::syn::value("{id:corpus:b,items:[{name:'one'},{name:'two'}],link:source:retained}")
            .unwrap();
    let nested = definition("DEFINE INDEX tuple_idx ON corpus FIELDS items.name,link");
    let result = fields(scalar_record_keys("ns", "db", &nested, &owner, &document, None).unwrap());
    assert_eq!(result.len(), 2);
    assert!(result
        .iter()
        .all(|tuple| tuple[1] == Value::Thing(Thing::from(("source", "retained")))));
    let remote = definition("DEFINE INDEX tuple_idx ON corpus FIELDS link.name");
    assert!(scalar_record_keys("ns", "db", &remote, &owner, &document, None).is_err());
    let id_document = crate::syn::value("{id:corpus:{part:'local'}}").unwrap();
    let Value::Thing(complex_owner) = crate::syn::value("corpus:{part:'local'}").unwrap() else {
        panic!("native complex identity");
    };
    let id_index = definition("DEFINE INDEX tuple_idx ON corpus FIELDS id.part");
    assert_eq!(
        fields(
            scalar_record_keys("ns", "db", &id_index, &complex_owner, &id_document, None).unwrap()
        ),
        vec![Array::from(vec![Value::from("local")])]
    );
}

#[test]
fn native_projection_rejects_dynamic_selected_fields_but_ignores_unrelated_fields() {
    let owner = Thing::from(("corpus", "b"));
    let document = crate::syn::value(
        "{id:corpus:b,name:'retained',unrelated:<future> { RETURN 'unrelated'; }}",
    )
    .unwrap();
    let local = definition("DEFINE INDEX tuple_idx ON corpus FIELDS name");
    assert_eq!(
        fields(scalar_record_keys("ns", "db", &local, &owner, &document, None).unwrap()),
        vec![Array::from(vec![Value::from("retained")])]
    );
    let dynamic = definition("DEFINE INDEX tuple_idx ON corpus FIELDS unrelated");
    assert!(scalar_record_keys("ns", "db", &dynamic, &owner, &document, None).is_err());
    let mut unknown_path = local.clone();
    unknown_path.cols.0[0]
        .0
        .push(Part::Start(Value::from("unbound")));
    assert!(scalar_record_keys("ns", "db", &unknown_path, &owner, &document, None).is_err());
}

#[test]
fn native_projection_maps_array_prefix_before_selecting_nested_suffix() {
    let owner = Thing::from(("corpus", "b"));
    let index = definition("DEFINE INDEX tuple_idx ON corpus FIELDS items.a[0].name");
    // Mapping .a creates [[one,two],[discarded]], so [0] selects the whole
    // FIRST nested array. The subsequent .name must inspect BOTH of its values.
    let document = crate::syn::value(
        "{id:corpus:b,items:[{a:[{name:'one'},{name:'two'}]},\
         {a:[{name:'discarded'}]}]}",
    )
    .unwrap();
    let result = fields(scalar_record_keys("ns", "db", &index, &owner, &document, None).unwrap());
    assert_eq!(
        result.into_iter().collect::<BTreeSet<_>>(),
        [
            Array::from(vec![Value::from("one")]),
            Array::from(vec![Value::from("two")]),
        ]
        .into_iter()
        .collect()
    );
    // The native Field+All special case consumes both parts before [0].
    let wildcard = definition("DEFINE INDEX tuple_idx ON corpus FIELDS items.a.*[0].name");
    assert_eq!(
        scalar_record_keys("ns", "db", &wildcard, &owner, &document, None).unwrap(),
        scalar_record_keys("ns", "db", &index, &owner, &document, None).unwrap()
    );
    for document in [
        "{id:corpus:b,items:[{a:[{name:'safe'},source:remote]}]}",
        "{id:corpus:b,items:[{a:[{name:'safe'},<future> { RETURN {name:'dynamic'}; }]}]}",
    ] {
        let document = crate::syn::value(document).unwrap();
        for index in [&index, &wildcard] {
            let error = scalar_record_keys("ns", "db", index, &owner, &document, None)
                .unwrap_err()
                .to_string();
            assert!(error.contains("immutable scalar proof"), "{error}");
        }
    }
    // Unlike direct id.part, mapping only id first yields a Thing. Accessing
    // .part afterward must not borrow the complex-ID local-object shortcut.
    let complex_ids =
        crate::syn::value("{id:corpus:b,items:[{id:source:{part:'local'}}]}").unwrap();
    let mapped_id = definition("DEFINE INDEX tuple_idx ON corpus FIELDS items.id.part");
    let error = scalar_record_keys("ns", "db", &mapped_id, &owner, &complex_ids, None)
        .unwrap_err()
        .to_string();
    assert!(error.contains("record dereference"), "{error}");
    let direct_index = definition("DEFINE INDEX tuple_idx ON corpus FIELDS items[0].id.part");
    assert_eq!(
        fields(scalar_record_keys("ns", "db", &direct_index, &owner, &complex_ids, None).unwrap()),
        vec![Array::from(vec![Value::from("local")])]
    );
    // Discarding a mapped sibling before accessing it remains valid: no
    // evaluation or dereference of this Future or remote Thing is requested.
    let discarded = crate::syn::value(
        "{id:corpus:b,items:[{a:[{name:'one'},{name:'two'}]},\
         {a:[source:remote,<future> { RETURN 'unrelated'; }]}]}",
    )
    .unwrap();
    assert_eq!(
        scalar_record_keys("ns", "db", &index, &owner, &discarded, None).unwrap(),
        scalar_record_keys("ns", "db", &index, &owner, &document, None).unwrap()
    );
}

#[test]
fn native_projection_binds_scope_table_kind_and_original_deadline() {
    let owner = Thing::from(("corpus", "b"));
    let document = crate::syn::value("{id:corpus:b,name:'retained'}").unwrap();
    let index = definition("DEFINE INDEX tuple_idx ON corpus FIELDS name");
    let keys = scalar_record_keys(
        "ns",
        "db",
        &index,
        &owner,
        &document,
        Some(Instant::now() + Duration::from_secs(5)),
    )
    .unwrap();
    assert_ne!(
        keys,
        scalar_record_keys("other", "db", &index, &owner, &document, None).unwrap()
    );
    assert!(
        scalar_record_keys("ns", "db", &index, &owner, &document, Some(Instant::now())).is_err()
    );
    assert!(scalar_record_keys(
        "ns",
        "db",
        &index,
        &Thing::from(("other", "b")),
        &document,
        None
    )
    .is_err());
    let aggregate = definition("DEFINE INDEX tuple_idx ON corpus FIELDS name HNSW DIMENSION 2");
    assert!(scalar_record_keys("ns", "db", &aggregate, &owner, &document, None).is_err());
}
