use mongreldb_core::catalog::Catalog;
use mongreldb_core::procedure::{
    ProcedureBody, ProcedureMode, ProcedureParam, ProcedureStep, ProcedureValue, StoredProcedure,
};
use mongreldb_core::TypeId;

#[test]
fn catalog_deserializes_without_procedures_field() {
    let json = r#"{
        "db_epoch": 7,
        "next_table_id": 3,
        "open_generation": 1,
        "next_segment_no": 2,
        "tables": []
    }"#;

    let catalog: Catalog = serde_json::from_str(json).unwrap();

    assert!(catalog.procedures.is_empty());
}

#[test]
fn procedure_checksum_is_stable_for_same_body() {
    let proc = sample_procedure("active_users");
    let same = sample_procedure("active_users");

    assert_eq!(proc.checksum, same.checksum);
    assert_eq!(proc.version, 1);
}

#[test]
fn procedure_validation_rejects_duplicate_params() {
    let mut proc = sample_procedure("active_users");
    proc.params.push(ProcedureParam {
        name: "status".into(),
        ty: TypeId::Bytes,
        nullable: false,
        default: None,
    });

    let err = proc.validate().unwrap_err().to_string();

    assert!(err.contains("duplicate procedure parameter"));
}

fn sample_procedure(name: &str) -> StoredProcedure {
    StoredProcedure::new(
        name,
        ProcedureMode::ReadOnly,
        vec![ProcedureParam {
            name: "status".into(),
            ty: TypeId::Bytes,
            nullable: false,
            default: None,
        }],
        ProcedureBody {
            steps: vec![ProcedureStep::NativeQuery {
                id: "read".into(),
                table: "users".into(),
                conditions: Vec::new(),
                projection: Some(vec![1, 2]),
                limit: Some(10),
            }],
            return_value: ProcedureValue::StepRows("read".into()),
        },
        0,
    )
    .unwrap()
}
