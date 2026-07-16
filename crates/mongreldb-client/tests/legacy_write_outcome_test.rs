use mongreldb_client::{ClientError, KitErrorCode, MongrelClient, ProcedureCallRequest, TxnOp};
use mongreldb_core::procedure::{ProcedureBody, ProcedureMode, ProcedureValue, StoredProcedure};
use mongreldb_core::{
    StoredTrigger, TriggerDefinition, TriggerEvent, TriggerProgram, TriggerTarget, TriggerTiming,
    Value,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn procedure(name: &str) -> StoredProcedure {
    procedure_returning(name, Value::Null)
}

fn procedure_returning(name: &str, value: Value) -> StoredProcedure {
    StoredProcedure::new(
        name,
        ProcedureMode::ReadOnly,
        Vec::new(),
        ProcedureBody {
            steps: Vec::new(),
            return_value: ProcedureValue::Literal(value),
        },
        0,
    )
    .unwrap()
}

fn trigger(name: &str) -> StoredTrigger {
    trigger_for(name, "users")
}

fn trigger_for(name: &str, table: &str) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table(table.into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram { steps: Vec::new() },
        },
        0,
    )
    .unwrap()
}

fn assert_unknown(error: ClientError) {
    assert!(matches!(
        error,
        ClientError::Kit {
            code: KitErrorCode::QueryOutcomeUnknown,
            committed: None,
            retryable: Some(false),
            ..
        }
    ));
}

#[tokio::test]
async fn malformed_legacy_write_successes_are_outcome_unknown() {
    let server = MockServer::start().await;
    for (verb, route) in [
        ("POST", "/tables"),
        ("DELETE", "/tables/users"),
        ("POST", "/tables/users/put"),
        ("POST", "/tables/users/commit"),
        ("POST", "/txn"),
        ("POST", "/kit/create_table"),
        ("POST", "/procedures"),
        ("PUT", "/procedures/p"),
        ("DELETE", "/procedures/p"),
        ("POST", "/procedures/p/call"),
        ("POST", "/kit/procedures/p/call"),
        ("POST", "/triggers"),
        ("PUT", "/triggers/t"),
        ("DELETE", "/triggers/t"),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
    }

    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri).unwrap();
        let procedure = procedure("p");
        let trigger = trigger("t");
        let call = ProcedureCallRequest {
            args: Default::default(),
            idempotency_key: None,
        };
        let errors = [
            client.create_table("users", Vec::new()).unwrap_err(),
            client.drop_table("users").unwrap_err(),
            client.put("users", Vec::new()).unwrap_err(),
            client.commit("users").unwrap_err(),
            client.txn(Vec::<TxnOp>::new()).unwrap_err(),
            client.kit_create_table(&serde_json::json!({})).unwrap_err(),
            client.create_procedure(procedure.clone()).unwrap_err(),
            client
                .replace_procedure("p", procedure.clone())
                .unwrap_err(),
            client.drop_procedure("p").unwrap_err(),
            client.call_procedure("p", &call).unwrap_err(),
            client.kit_call_procedure("p", &call).unwrap_err(),
            client.create_trigger(trigger.clone()).unwrap_err(),
            client.replace_trigger("t", trigger.clone()).unwrap_err(),
            client.drop_trigger("t").unwrap_err(),
        ];
        for error in errors {
            assert_unknown(error);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn exact_legacy_commit_receipts_are_accepted() {
    let server = MockServer::start().await;
    for (verb, route, epoch) in [
        ("DELETE", "/tables/users", 11_u64),
        ("POST", "/txn", 12),
        ("DELETE", "/procedures/p", 13),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "committed",
                "epoch": epoch,
                "epoch_text": epoch.to_string(),
            })))
            .expect(1)
            .mount(&server)
            .await;
    }
    let dropped_trigger = trigger("t");
    Mock::given(method("DELETE"))
        .and(path("/triggers/t"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "committed",
            "epoch": 14,
            "epoch_text": "14",
            "dropped_trigger": dropped_trigger,
            "resource_tables": [],
        })))
        .expect(1)
        .mount(&server)
        .await;

    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri).unwrap();
        client.drop_table("users").unwrap();
        client.txn(Vec::<TxnOp>::new()).unwrap();
        client.drop_procedure("p").unwrap();
        client.drop_trigger("t").unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn procedure_and_trigger_receipts_are_bound_to_the_request() {
    let server = MockServer::start().await;
    let mut other_procedure = procedure_returning("p", Value::Int64(1));
    other_procedure.created_epoch = 7;
    other_procedure.updated_epoch = 7;
    let mut other_trigger = trigger_for("t", "other");
    other_trigger.created_epoch = 8;
    other_trigger.updated_epoch = 8;
    for (verb, route, body) in [
        (
            "POST",
            "/procedures",
            serde_json::json!({"status": "ok", "procedure": other_procedure.clone()}),
        ),
        (
            "PUT",
            "/procedures/p",
            serde_json::json!({"status": "ok", "procedure": other_procedure}),
        ),
        (
            "POST",
            "/triggers",
            serde_json::json!({"status": "ok", "trigger": other_trigger.clone()}),
        ),
        (
            "PUT",
            "/triggers/t",
            serde_json::json!({"status": "ok", "trigger": other_trigger}),
        ),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
    }

    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri).unwrap();
        let expected_procedure = procedure("p");
        let expected_trigger = trigger("t");
        for error in [
            client
                .create_procedure(expected_procedure.clone())
                .unwrap_err(),
            client
                .replace_procedure("p", expected_procedure)
                .unwrap_err(),
            client.create_trigger(expected_trigger.clone()).unwrap_err(),
            client.replace_trigger("t", expected_trigger).unwrap_err(),
        ] {
            assert_unknown(error);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn procedure_call_receipt_explicitly_proves_commit_state() {
    for (body, accepted) in [
        (
            serde_json::json!({
                "status": "ok",
                "committed": false,
                "epoch": null,
                "epoch_text": null,
                "result": null,
            }),
            true,
        ),
        (
            serde_json::json!({
                "status": "ok",
                "committed": true,
                "epoch": 9,
                "epoch_text": "9",
                "result": null,
            }),
            true,
        ),
        (
            serde_json::json!({
                "status": "ok",
                "epoch": 9,
                "epoch_text": "9",
                "result": null,
            }),
            false,
        ),
        (
            serde_json::json!({
                "status": "ok",
                "committed": false,
                "epoch": 9,
                "epoch_text": "9",
                "result": null,
            }),
            false,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/procedures/p/call"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let result = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&uri).unwrap().call_procedure(
                "p",
                &ProcedureCallRequest {
                    args: Default::default(),
                    idempotency_key: None,
                },
            )
        })
        .await
        .unwrap();
        if accepted {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert_unknown(result.unwrap_err());
        }
    }
}

#[test]
fn legacy_write_transport_loss_is_outcome_unknown() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let client = MongrelClient::new(&format!("http://{address}")).unwrap();
    assert_unknown(client.create_table("users", Vec::new()).unwrap_err());
}
