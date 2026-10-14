//! Physical transport controls. These are not semantic/provider verdicts.
use super::*;

fn request(kind: &str, space: &str, sequence: usize) -> Request {
    Request {
        format: REQUEST_FORMAT.into(),
        request_id: digest(format!("{kind}:{space}:{sequence}").as_bytes()),
        run_nonce: "page-reuse-original-test".into(),
        backend: "surreal-db".into(),
        space: space.into(),
        kind: kind.into(),
        selector: json!({"controlled_transport_fixture":true}),
    }
}

fn rows(space: &str, count: usize) -> Vec<Value> {
    (0..count)
        .map(|i| {
            json!({"id":format!("entity:{i:06}"),"space_id":space,
            "deleted_at":null,"security":{"level":0},"superseded":false})
        })
        .collect()
}

fn prepare(request: &Request, rows: Vec<Value>, allocation: &Allocation) -> Result<PreparedPacket> {
    prepare_packet(
        request,
        &encode(request)?,
        Packet {
            evidence: json!({"complete":true}),
            tables: BTreeMap::from([("entity".into(), rows)]),
        },
        None,
        allocation,
    )
}

#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn descriptors(packet: &PreparedPacket) -> Vec<Value> {
    serde_json::from_slice::<Value>(&packet.reply).unwrap()["pages"]
        .as_array()
        .unwrap()
        .clone()
}

fn published(
    packet: PreparedPacket,
    allocation: &mut Allocation,
    files: &mut BTreeMap<String, Vec<u8>>,
) {
    for (name, raw) in packet.pages {
        assert!(
            files.insert(name, raw).is_none(),
            "original page overwritten"
        );
    }
    allocation.bytes += packet.bytes;
    allocation.rows += packet.rows;
    allocation.calls.extend(packet.calls);
    allocation.maintenance_rows.extend(packet.maintenance_rows);
}

#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn reconstruct(descriptors: &[Value], files: &BTreeMap<String, Vec<u8>>) -> Vec<Value> {
    let mut ordered = BTreeMap::new();
    let mut result = Vec::new();
    for descriptor in descriptors {
        let raw = &files[descriptor["name"].as_str().unwrap()];
        assert_eq!(descriptor["sha256"], digest(raw));
        assert_eq!(descriptor["bytes"], raw.len());
        let page: Vec<Value> = serde_json::from_slice(raw).unwrap();
        assert_eq!(descriptor["rows"], page.len());
        assert!(page.len() <= MAX_PAGE_ROWS);
        if let Some(selection) = descriptor.get("selection") {
            for pair in selection.as_array().unwrap() {
                let offset = usize::try_from(pair[0].as_u64().unwrap()).unwrap();
                let position = usize::try_from(pair[1].as_u64().unwrap()).unwrap();
                assert!(ordered.insert(position, page[offset].clone()).is_none());
            }
        } else {
            result.extend(page);
        }
    }
    if !ordered.is_empty() {
        assert!(result.is_empty());
        assert_eq!(
            ordered.keys().copied().collect::<Vec<_>>(),
            (0..ordered.len()).collect::<Vec<_>>()
        );
        result.extend(ordered.into_values());
    }
    result
}

#[test]
#[expect(
    clippy::assertions_on_constants,
    reason = "Retain the independent original row-limit assertion as part of the runtime fixture oracle."
)]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn canonical_snapshots_reuse_exact_rows_without_duplicate_physical_rows() {
    let mut allocation = Allocation::default();
    let mut files = BTreeMap::new();
    let mut original_descriptors = BTreeMap::new();
    for (i, &space) in SPACES.iter().enumerate() {
        let snapshot = rows(space, 1100);
        let prepared = prepare(
            &request("original_maintenance", space, i),
            snapshot.clone(),
            &allocation,
        )
        .unwrap();
        assert_eq!(prepared.rows, 1100);
        original_descriptors.insert(space, descriptors(&prepared));
        published(prepared, &mut allocation, &mut files);
        assert_eq!(reconstruct(&original_descriptors[space], &files), snapshot);
    }
    let original_files = files.clone();
    assert_eq!(allocation.rows, 2200);
    // The four logical snapshots contain4400 rows. The same original pages
    // need only2200 physical rows; the4096 physical-row ceiling is unchanged.
    assert!(4 * 1100 > MAX_ROWS);
    for (i, &space) in SPACES.iter().enumerate() {
        let current = request("current_canonical", space, i);
        let prepared = prepare(&current, rows(space, 1100), &allocation).unwrap();
        assert_eq!(prepared.rows, 0);
        assert!(prepared.pages.is_empty());
        assert_eq!(
            descriptors(&prepared).len(),
            original_descriptors[space].len()
        );
        for (current, original) in descriptors(&prepared)
            .iter()
            .zip(&original_descriptors[space])
        {
            assert_eq!(current["name"], original["name"]);
            assert_eq!(current["sha256"], original["sha256"]);
            assert!(current["selection"].is_array());
        }
        assert_eq!(
            prepared.bytes,
            encode(&current).unwrap().len() + prepared.reply.len()
        );
        assert_eq!(
            reconstruct(&descriptors(&prepared), &files),
            rows(space, 1100)
        );
        published(prepared, &mut allocation, &mut files);
    }
    assert_eq!(allocation.rows, 2200);
    assert_eq!(files, original_files);
    assert!(allocation.bytes <= MAX_TOTAL_BYTES);
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn mixed_snapshot_preserves_original_pages_and_charges_only_changed_row() {
    let space = SPACES[0];
    let mut allocation = Allocation::default();
    let mut files = BTreeMap::new();
    let original = rows(space, 256);
    let prepared = prepare(
        &request("original_maintenance", space, 0),
        original.clone(),
        &allocation,
    )
    .unwrap();
    let original_refs = descriptors(&prepared);
    published(prepared, &mut allocation, &mut files);
    let original_files = files.clone();
    let mut changed = original;
    changed[128]["deleted_at"] = json!("2026-10-11T00:00:00Z");
    let current = request("current_canonical", space, 0);
    let prepared = prepare(&current, changed.clone(), &allocation).unwrap();
    let current_refs = descriptors(&prepared);
    assert_eq!(current_refs[0]["name"], original_refs[0]["name"]);
    assert_eq!(current_refs[1]["name"], original_refs[1]["name"]);
    assert_ne!(current_refs[2]["sha256"], original_refs[1]["sha256"]);
    assert_eq!(prepared.pages.len(), 1);
    assert_eq!(prepared.rows, 1);
    assert_eq!(
        prepared.pages[0].0,
        format!("{}.0002.json", current.request_id)
    );
    assert_eq!(current_refs[2]["name"], prepared.pages[0].0);
    assert_eq!(
        prepared.bytes,
        encode(&current).unwrap().len() + prepared.reply.len() + prepared.pages[0].1.len()
    );
    published(prepared, &mut allocation, &mut files);
    assert_eq!(reconstruct(&current_refs, &files), changed);
    for (name, raw) in original_files {
        assert_eq!(files[&name], raw);
    }
    assert_eq!(allocation.rows, 257);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn changed_foreign_and_unpublished_pages_cannot_borrow_exhausted_capacity() {
    let space = SPACES[0];
    let original = rows(space, MAX_ROWS);
    let mut allocation = Allocation::default();
    let mut files = BTreeMap::new();
    let prepared = prepare(
        &request("original_maintenance", space, 0),
        original.clone(),
        &allocation,
    )
    .unwrap();
    assert!(
        allocation.maintenance_rows.is_empty(),
        "preparation cannot publish reference authority"
    );
    let unpublished = Allocation {
        rows: MAX_ROWS,
        ..Default::default()
    };
    assert_eq!(
        prepare(
            &request("current_canonical", space, 0),
            original.clone(),
            &unpublished
        )
        .err()
        .unwrap()
        .0,
        "capture_row_limit"
    );
    published(prepared, &mut allocation, &mut files);
    let before = files.clone();
    assert_eq!(
        prepare(
            &request("current_canonical", space, 0),
            original.clone(),
            &allocation
        )
        .unwrap()
        .rows,
        0
    );
    for field in ["deleted_at", "security", "superseded"] {
        let mut changed = original.clone();
        changed[0][field] = json!({"changed_original":field});
        assert_eq!(
            prepare(
                &request("current_canonical", space, 0),
                changed,
                &allocation
            )
            .err()
            .unwrap()
            .0,
            "capture_row_limit"
        );
    }
    // Even byte-identical content cannot borrow another named space's pages.
    assert_eq!(
        prepare(
            &request("current_canonical", SPACES[1], 0),
            original.clone(),
            &allocation
        )
        .err()
        .unwrap()
        .0,
        "capture_row_limit"
    );
    assert_eq!(
        prepare(
            &request("original_publication", space, 0),
            original.clone(),
            &allocation
        )
        .err()
        .unwrap()
        .0,
        "capture_row_limit"
    );
    let mut excessive = original.clone();
    excessive.push(json!({"id":"entity:extra"}));
    assert_eq!(
        prepare(
            &request("current_canonical", space, 0),
            excessive,
            &allocation
        )
        .err()
        .unwrap()
        .0,
        "capture_row_limit"
    );
    allocation.bytes = MAX_TOTAL_BYTES;
    assert_eq!(
        prepare(
            &request("current_canonical", space, 0),
            original,
            &allocation
        )
        .err()
        .unwrap()
        .0,
        "capture_total_byte_limit"
    );
    assert_eq!(allocation.rows, MAX_ROWS);
    assert_eq!(files, before);
}

#[test]
#[expect(
    clippy::redundant_clone,
    reason = "Keep the original snapshot and its independently mutated copy explicit in the retained row-custody regression."
)]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn insertions_and_deleted_rows_do_not_shift_immutable_evidence_authority() {
    let space = SPACES[0];
    let mut allocation = Allocation::default();
    let mut files = BTreeMap::new();
    let original = rows(space, 1274);
    published(
        prepare(
            &request("original_maintenance", space, 0),
            original.clone(),
            &allocation,
        )
        .unwrap(),
        &mut allocation,
        &mut files,
    );
    let before = files.clone();
    // Insert between existing IDs across every old128-row boundary; physically
    // remove three rows and change policy/tombstone fields of two others.
    let mut current = original.clone();
    current.retain(|row| {
        !["entity:000001", "entity:000127", "entity:000900"].contains(&row["id"].as_str().unwrap())
    });
    current[130]["security"] = json!({"level":5,"subjects":["restricted"]});
    current[250]["deleted_at"] = json!("2026-10-11T00:00:00Z");
    for index in 0..390 {
        current.push(json!({"id":format!("entity:{index:06}:inserted"),"space_id":space}));
    }
    current.sort_by_key(|row| row["id"].as_str().unwrap().to_owned());
    assert_eq!(current.len(), 1661);
    let prepared = prepare(
        &request("current_canonical", space, 0),
        current.clone(),
        &allocation,
    )
    .unwrap();
    assert_eq!(prepared.rows, 392);
    let references = descriptors(&prepared);
    published(prepared, &mut allocation, &mut files);
    assert_eq!(reconstruct(&references, &files), current);
    assert_eq!(allocation.rows, 1666);
    for (name, raw) in before {
        assert_eq!(files[&name], raw);
    }
    // All1262 missing business rows and729 original business rows still fit.
    assert!(allocation.rows + 1262 + 729 <= MAX_ROWS);
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn current_snapshots_and_other_tables_cannot_grant_maintenance_authority() {
    let space = SPACES[0];
    let original = rows(space, 4);
    let mut allocation = Allocation::default();
    let mut files = BTreeMap::new();
    published(
        prepare(
            &request("current_canonical", space, 0),
            original.clone(),
            &allocation,
        )
        .unwrap(),
        &mut allocation,
        &mut files,
    );
    assert!(allocation.maintenance_rows.is_empty());
    let prepared = prepare(
        &request("current_canonical", space, 1),
        original.clone(),
        &allocation,
    )
    .unwrap();
    assert_eq!(prepared.rows, 4);
    published(
        prepare(
            &request("original_maintenance", space, 0),
            original.clone(),
            &allocation,
        )
        .unwrap(),
        &mut allocation,
        &mut files,
    );
    let req = request("current_canonical", space, 2);
    let prepared = prepare_packet(
        &req,
        &encode(&req).unwrap(),
        Packet {
            evidence: json!({"complete":true}),
            tables: BTreeMap::from([("assertion".into(), original)]),
        },
        None,
        &allocation,
    )
    .unwrap();
    assert_eq!(
        prepared.rows, 4,
        "byte equality across tables is not authority"
    );
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn row_reuse_still_enforces_reply_and_page_byte_limits() {
    let space = SPACES[0];
    let allocation = Allocation::default();
    let req = request("current_canonical", space, 0);
    let oversized = vec![json!({"id":"entity:large","value":"x".repeat(MAX_PAGE_BYTES)})];
    let fragmented = prepare(&req, oversized, &allocation).unwrap();
    assert_eq!(fragmented.rows, 1);
    assert!(fragmented.pages.len() > 1);
    assert!(fragmented
        .pages
        .iter()
        .all(|(_, raw)| raw.len() <= MAX_PAGE_BYTES));
    assert!(descriptors(&fragmented)
        .iter()
        .all(|page| page.get("row_fragment").is_some()));
    let packet = Packet {
        evidence: json!({"complete":true,"value":"x".repeat(MAX_REPLY_BYTES)}),
        tables: BTreeMap::new(),
    };
    assert_eq!(
        prepare_packet(&req, &encode(&req).unwrap(), packet, None, &allocation)
            .err()
            .unwrap()
            .0,
        "capture_reply_byte_limit"
    );
}
