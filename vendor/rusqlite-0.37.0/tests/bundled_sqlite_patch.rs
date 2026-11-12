#[test]
fn bundled_sqlite_includes_the_wal_page_size_security_fix() {
    assert!(
        rusqlite::version_number() >= 3_054_000,
        "bundled SQLite must include the RUSTSEC-2026-0459 fix; linked {}",
        rusqlite::version()
    );
}
