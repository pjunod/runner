use nzbd_state::artifacts::Inventory;
use rusqlite::Connection;

#[test]
fn missing_enrolled_inventory_is_not_created() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("missing-state");
    let expected = "enrolled-installation".to_owned();

    assert!(Inventory::open_expected(&state_dir, Some(&expected)).is_err());
    assert!(!state_dir.exists());
    assert!(!state_dir.join("artifacts.sqlite").exists());
    assert!(!state_dir.join("artifacts.lock").exists());
}

#[test]
fn mismatched_enrollment_preserves_installation_and_schema() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let inventory = Inventory::open(&state_dir).unwrap();
    let enrolled_id = inventory.installation.clone();
    drop(inventory);

    let (installation_before, schema_before) = {
        let db = Connection::open(state_dir.join("artifacts.sqlite")).unwrap();
        let installation: String = db
            .query_row(
                "SELECT value FROM meta WHERE key='installation'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let schema: String = db
            .query_row("SELECT value FROM meta WHERE key='schema'", [], |row| {
                row.get(0)
            })
            .unwrap();
        (installation, schema)
    };
    let wrong_id = format!("{enrolled_id}-other");
    assert!(Inventory::open_expected(&state_dir, Some(&wrong_id)).is_err());

    let db = Connection::open(state_dir.join("artifacts.sqlite")).unwrap();
    let installation_after: String = db
        .query_row(
            "SELECT value FROM meta WHERE key='installation'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let schema_after: String = db
        .query_row("SELECT value FROM meta WHERE key='schema'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(installation_after, installation_before);
    assert_eq!(schema_after, schema_before);
}

#[test]
fn matching_enrollment_opens_existing_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let inventory = Inventory::open(&state_dir).unwrap();
    let enrolled_id = inventory.installation.clone();
    drop(inventory);

    let reopened = Inventory::open_expected(&state_dir, Some(&enrolled_id)).unwrap();
    assert_eq!(reopened.installation, enrolled_id);
    drop(reopened);
}

#[test]
fn legacy_open_still_creates_and_opens_an_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");

    let inventory = Inventory::open(&state_dir).unwrap();
    assert!(!inventory.installation.is_empty());
    drop(inventory);

    let reopened = Inventory::open(&state_dir).unwrap();
    assert_eq!(reopened.installation.len(), 32);
}
