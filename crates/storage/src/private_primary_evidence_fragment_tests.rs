//! Exact-byte transport controls, not provider or semantic acceptance.
use super::*;

fn request(kind: &str, sequence: usize) -> Request {
    Request {
        format: REQUEST_FORMAT.into(),
        request_id: digest(format!("fragments:{kind}:{sequence}").as_bytes()),
        run_nonce: "original-fragment-control".into(),
        backend: "elephant-db".into(),
        space: SPACES[0].into(),
        kind: kind.into(),
        selector: json!({"controlled_transport_fixture":true}),
    }
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

#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn reconstruct_group(group: &[Value], files: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
    let first = &group[0]["row_fragment"];
    assert_eq!(first["count"], group.len());
    let mut original = Vec::new();
    for (index, descriptor) in group.iter().enumerate() {
        let fragment = &descriptor["row_fragment"];
        assert_eq!(fragment["index"], index);
        assert_eq!(fragment["offset"], original.len());
        assert_eq!(fragment["count"], first["count"]);
        assert_eq!(fragment["row_bytes"], first["row_bytes"]);
        assert_eq!(fragment["row_sha256"], first["row_sha256"]);
        assert_eq!(descriptor["table"], group[0]["table"]);
        assert_eq!(descriptor["rows"], 1);
        assert_eq!(descriptor.get("selection"), group[0].get("selection"));
        let raw = &files[descriptor["name"].as_str().unwrap()];
        assert!(raw.len() <= MAX_PAGE_BYTES);
        assert_eq!(descriptor["bytes"], raw.len());
        assert_eq!(descriptor["sha256"], digest(raw));
        let body: Value = serde_json::from_slice(raw).unwrap();
        assert_eq!(body.as_object().unwrap().len(), 2);
        assert_eq!(body["encoding"], "base64");
        original.extend(BASE64.decode(body["data"].as_str().unwrap()).unwrap());
    }
    assert_eq!(first["row_bytes"], original.len());
    assert_eq!(first["row_sha256"], digest(&original));
    original
}

#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn large_row() -> Value {
    // The non-BMP scalar crosses a raw fragment boundary. Each page carries
    // bytes, so its slice need not itself be valid UTF-8 or standalone JSON.
    let empty = json!({"id":"entity:large","n":-0.0,"text":""});
    let prefix = encode(&empty).unwrap();
    let text_start = prefix.windows(8).position(|w| w == b"\"text\":\"").unwrap() + 8;
    let padding = FRAGMENT_BYTES - text_start - 1;
    json!({"id":"entity:large","n":-0.0,
        "text":format!("{}🦦{}", "x".repeat(padding), "y".repeat(MAX_PAGE_BYTES))})
}

// Root can request one private output from this already selected regression.
// The consumer reads these actual production bytes; it does not recreate them.
#[cfg(unix)]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn write_roundtrip_fixture(request: &Request, packet: &PreparedPacket, original_table: &[u8]) {
    use std::io::Write;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let Some(directory) = std::env::var_os("RECALL_PRIVATE_FRAGMENT_FIXTURE_DIR") else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    assert!(directory.is_absolute());
    let parent = directory.parent().unwrap();
    assert_eq!(std::fs::canonicalize(parent).unwrap(), parent);
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .unwrap();
    let mut files = BTreeMap::new();
    let mut write = |name: &str, raw: &[u8]| {
        assert!(raw.len() <= MAX_TOTAL_BYTES);
        assert_eq!(std::path::Path::new(name).components().count(), 1);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join(name))
            .unwrap();
        file.write_all(raw).unwrap();
        file.sync_all().unwrap();
        files.insert(
            name.to_owned(),
            json!({"sha256":digest(raw),"bytes":raw.len()}),
        );
    };
    write("request.json", &encode(request).unwrap());
    write("response.json", &packet.reply);
    write("expected-table.json", original_table);
    for (name, raw) in &packet.pages {
        write(name, raw);
    }
    let manifest = encode(
        &json!({"format":"recall-private-fragment-producer-fixture/v1",
        "producer":"prepare_packet","table":"entity","files":files}),
    )
    .unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.join("fixture.json"))
        .unwrap();
    file.write_all(&manifest).unwrap();
    file.sync_all().unwrap();
    std::fs::File::open(&directory).unwrap().sync_all().unwrap();
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn fragments_preserve_original_json_bytes_between_ordinary_rows() {
    let original = large_row();
    let rows = vec![
        json!({"id":"entity:before"}),
        original.clone(),
        json!({"id":"entity:later"}),
    ];
    #[cfg(unix)]
    let original_table = encode(&rows).unwrap();
    let original_request = request("original_maintenance", 0);
    let packet = prepare(&original_request, rows, &Allocation::default()).unwrap();
    #[cfg(unix)]
    write_roundtrip_fixture(&original_request, &packet, &original_table);
    let descriptors = descriptors(&packet);
    assert_eq!(packet.rows, 3, "fragments count as one logical row");
    assert!(descriptors.first().unwrap().get("row_fragment").is_none());
    assert!(descriptors.last().unwrap().get("row_fragment").is_none());
    let group = &descriptors[1..descriptors.len() - 1];
    let files: BTreeMap<_, _> = packet.pages.into_iter().collect();
    let raw = reconstruct_group(group, &files);
    assert_eq!(raw, encode(&original).unwrap());
    assert_eq!(serde_json::from_slice::<Value>(&raw).unwrap(), original);
    assert!(group.iter().any(|descriptor| {
        let body: Value =
            serde_json::from_slice(&files[descriptor["name"].as_str().unwrap()]).unwrap();
        std::str::from_utf8(&BASE64.decode(body["data"].as_str().unwrap()).unwrap()).is_err()
    }));
    assert_eq!(
        packet.maintenance_rows.len(),
        2,
        "fragmented rows cannot seed reuse authority"
    );
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn current_fragment_groups_are_fresh_and_select_one_original_position() {
    let row = large_row();
    let rows = vec![
        json!({"id":"entity:before"}),
        row.clone(),
        json!({"id":"entity:later"}),
    ];
    let mut allocation = Allocation::default();
    let maintenance = prepare(
        &request("original_maintenance", 0),
        rows.clone(),
        &allocation,
    )
    .unwrap();
    allocation.bytes += maintenance.bytes;
    allocation.rows += maintenance.rows;
    allocation
        .maintenance_rows
        .extend(maintenance.maintenance_rows);
    let current_request = request("current_canonical", 1);
    let current = prepare(&current_request, rows, &allocation).unwrap();
    assert_eq!(
        current.rows, 1,
        "only the fragmented row is physically recaptured"
    );
    let all = descriptors(&current);
    let group: Vec<_> = all
        .iter()
        .filter(|page| page.get("row_fragment").is_some())
        .cloned()
        .collect();
    for page in &group {
        assert_eq!(page["selection"], json!([[0, 1]]));
        assert!(page["name"]
            .as_str()
            .unwrap()
            .starts_with(&current_request.request_id));
    }
    assert_eq!(
        current.pages.len(),
        group.len(),
        "ordinary unchanged rows retain exact earlier pages"
    );
    let files = current.pages.into_iter().collect();
    assert_eq!(reconstruct_group(&group, &files), encode(&row).unwrap());
    assert!(current.maintenance_rows.is_empty());
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn fragments_do_not_reset_page_total_or_logical_row_limits() {
    let request = request("original_maintenance", 0);
    let too_many_pages = json!({"id":"entity:large","text":"x".repeat(FRAGMENT_BYTES * MAX_PAGES)});
    assert_eq!(
        prepare(&request, vec![too_many_pages], &Allocation::default())
            .err()
            .unwrap()
            .0,
        "page_count_limit"
    );
    let row = large_row();
    let first = prepare(&request, vec![row.clone()], &Allocation::default()).unwrap();
    let bytes = Allocation {
        bytes: MAX_TOTAL_BYTES - first.bytes + 1,
        ..Allocation::default()
    };
    assert_eq!(
        prepare(&request, vec![row.clone()], &bytes)
            .err()
            .unwrap()
            .0,
        "capture_total_byte_limit"
    );
    let rows = Allocation {
        rows: MAX_ROWS,
        ..Allocation::default()
    };
    assert_eq!(
        prepare(&request, vec![row], &rows).err().unwrap().0,
        "capture_row_limit"
    );
    let oversized = json!({"id":"entity:large","text":"x".repeat(MAX_TOTAL_BYTES)});
    assert_eq!(
        prepare(&request, vec![oversized], &Allocation::default())
            .err()
            .unwrap()
            .0,
        "capture_total_byte_limit"
    );
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Malformed fixture data or a failed operation must fail this regression at its original assertion site."
)]
fn exact_page_boundary_uses_ordinary_then_fragmented_representation() {
    let empty = json!({"id":"entity:boundary","text":""});
    let overhead = encode(&empty).unwrap().len();
    for (extra, fragmented) in [(0, false), (1, true)] {
        let row = json!({"id":"entity:boundary","text":"x".repeat(MAX_PAGE_BYTES - 2 - overhead + extra)});
        let original = encode(&row).unwrap();
        assert_eq!(original.len(), MAX_PAGE_BYTES - 2 + extra);
        let packet = prepare(
            &request("original_maintenance", extra),
            vec![row],
            &Allocation::default(),
        )
        .unwrap();
        assert_eq!(packet.rows, 1);
        let descriptors = descriptors(&packet);
        assert_eq!(descriptors[0].get("row_fragment").is_some(), fragmented);
        if fragmented {
            assert_eq!(
                reconstruct_group(&descriptors, &packet.pages.into_iter().collect()),
                original
            );
        } else {
            assert_eq!(packet.pages.len(), 1);
            assert_eq!(packet.pages[0].1.len(), MAX_PAGE_BYTES);
        }
    }
}
