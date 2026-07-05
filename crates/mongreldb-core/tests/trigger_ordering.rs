use mongreldb_core::{
    ColumnDef, ColumnFlags, Database, Schema, StoredTrigger, TriggerCell, TriggerDefinition,
    TriggerEvent, TriggerProgram, TriggerStep, TriggerTarget, TriggerTiming, TriggerValue, TypeId,
    Value,
};
use tempfile::tempdir;

fn base_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "note".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn set_note_trigger(name: &str, note: &str) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table("base".into()),
            timing: TriggerTiming::Before,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::SetNew {
                    cells: vec![TriggerCell {
                        column_id: 2,
                        value: TriggerValue::Literal(Value::Bytes(note.as_bytes().to_vec())),
                    }],
                }],
            },
        },
        0,
    )
    .unwrap()
}

fn insert_base(db: &Database, id: i64) {
    db.transaction(|tx| {
        tx.put(
            "base",
            vec![
                (1, Value::Int64(id)),
                (2, Value::Bytes(b"original".to_vec())),
            ],
        )
        .map(|_| ())
    })
    .unwrap();
}

fn note_for_id(db: &Database, id: i64) -> String {
    let table = db.table("base").unwrap();
    let guard = table.lock();
    let rows = guard.visible_rows(guard.snapshot()).unwrap();
    let row = rows
        .iter()
        .find(|row| row.columns.get(&1) == Some(&Value::Int64(id)))
        .unwrap();
    match row.columns.get(&2).unwrap() {
        Value::Bytes(bytes) => String::from_utf8(bytes.clone()).unwrap(),
        other => panic!("expected bytes note, got {other:?}"),
    }
}

#[test]
fn triggers_fire_in_creation_order_and_replacement_keeps_position() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("base", base_schema()).unwrap();

    db.create_trigger(set_note_trigger("first", "first"))
        .unwrap();
    db.create_trigger(set_note_trigger("second", "second"))
        .unwrap();

    insert_base(&db, 1);
    assert_eq!(note_for_id(&db, 1), "second");

    db.create_or_replace_trigger(set_note_trigger("first", "replaced"))
        .unwrap();
    insert_base(&db, 2);
    assert_eq!(note_for_id(&db, 2), "second");

    let triggers = db.triggers();
    assert_eq!(triggers[0].name, "first");
    assert_eq!(triggers[1].name, "second");
    assert!(triggers[0].created_epoch < triggers[1].created_epoch);
}
